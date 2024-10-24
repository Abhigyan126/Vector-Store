use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;
use std::io::{self};
use std::time::Instant;
use std::path::Path;
use serde_json::json;


mod kdtree;
use kdtree::{KDTree, Point, Node};

struct APPState {
    trees: Mutex<HashMap<String, KDTreeCache>>, // Tree cache keyed by name
    max_memory_usage: usize, // Maximum memory allowed for in-memory trees
}
#[derive(Debug)]
struct KDTreeCache {
    tree: Option<KDTree>, // The actual KDTree (None means it's offloaded)
    last_accessed: Instant, // Time when the tree was last accessed
}

#[derive(Deserialize)]
struct QueryParams {
    tree_name: String, // Name of the tree
    n: Option<usize>, // Optional number of nearest neighbors to find
}

fn load_tree(tree_name: &str) -> io::Result<KDTree> {
    let file_name = format!("{}.bin", tree_name);
    if !Path::new(&file_name).exists() {
        return Err(io::Error::new(io::ErrorKind::NotFound, format!("File not found: {}", file_name)));
    }
    println!("Loading KDTree from file: {}", file_name); // Logging
    KDTree::load_from_file(&file_name)
}

fn offload_tree(tree_name: &str, tree: &KDTree) -> io::Result<()> {
    let file_name = format!("{}.bin", tree_name);
    println!("Saving KDTree to file: {}", file_name); // Logging
    tree.save_to_file(&file_name)
}


fn estimate_memory_usage(tree: &KDTree) -> usize {
    let mut total_size = 0;

    // Size of the KDTree struct itself
    total_size += size_of::<KDTree>();

    // If the root exists, we need to calculate the size of the entire tree
    if let Some(root) = &tree.root {
        total_size += estimate_node_size(&root);
    }

    total_size
}

fn estimate_node_size(node: &Box<Node>) -> usize {
    let mut total_size = 0;

    // Size of the node itself (including its point and axis)
    total_size += size_of_val(node);

    // Estimate size of the left and right children recursively
    if let Some(left_child) = &node.left {
        total_size += estimate_node_size(&left_child);
    }
    if let Some(right_child) = &node.right {
        total_size += estimate_node_size(&right_child);
    }

    total_size
}

fn manage_memory(trees: &mut HashMap<String, KDTreeCache>, max_memory_usage: usize) {
    let mut total_memory_usage = 0;

    // Step 1: Calculate the total memory usage of currently loaded trees
    for cache in trees.values() {
        if let Some(tree) = &cache.tree {
            total_memory_usage += estimate_memory_usage(tree);
        }
    }

    // Step 2: Trigger LRU logic if total memory usage exceeds the limit
    while total_memory_usage > max_memory_usage {
        // Find the least recently used tree
        let mut least_recently_used: Option<(String, &KDTreeCache)> = None;
        for (key, cache) in trees.iter() {
            if cache.tree.is_some() {
                if let Some((_, lru_cache)) = &least_recently_used {
                    if cache.last_accessed < lru_cache.last_accessed {
                        least_recently_used = Some((key.clone(), cache));
                    }
                } else {
                    least_recently_used = Some((key.clone(), cache));
                }
            }
        }

        // Offload the least recently used tree
        if let Some((tree_name, _)) = least_recently_used {
            if let Some(cache) = trees.get_mut(&tree_name) {
                if let Some(tree) = cache.tree.take() {
                    offload_tree(&tree_name, &tree).unwrap();
                    total_memory_usage -= estimate_memory_usage(&tree);
                }
            }
        } else {
            break; // No trees left to offload
        }
    }
}



// API route to insert a point into a specific KD-Tree
async fn insert_point(
    data: web::Json<Point>, 
    query: web::Query<QueryParams>, 
    state: web::Data<APPState>
) -> impl Responder {
    let mut trees = state.trees.lock().unwrap();
    let tree_name = &query.tree_name;

    // Check if the tree exists in memory or needs to be created
    let cache = trees.entry(tree_name.clone()).or_insert_with(|| {
        let new_tree = KDTree::new(data.0.len()); // Create a new tree with the dimension of the incoming point
        KDTreeCache {
            tree: Some(new_tree),
            last_accessed: Instant::now(),
        }
    });

    // Load the tree from disk if it exists but is not loaded yet
    if cache.tree.is_none() {
        cache.tree = Some(load_tree(tree_name).unwrap());
    }

    // Update last accessed time
    cache.last_accessed = Instant::now();

    if let Some(ref mut tree) = cache.tree {
        // Insert the point into the KD-Tree
        tree.insert(data.into_inner());

        // Immediately save the tree to disk after insertion
        if let Err(e) = offload_tree(tree_name, tree) {
            return HttpResponse::InternalServerError().body(format!("Failed to save KD-Tree: {}", e));
        }

        // Manage memory
        manage_memory(&mut trees, state.max_memory_usage);
        HttpResponse::Ok().json("Point inserted into KD-Tree and saved to disk")
    } else {
        HttpResponse::InternalServerError().body("Failed to load or create KD-Tree")
    }
}




async fn nearest_neighbor_top_n(
    data: web::Json<Point>, 
    query: web::Query<QueryParams>, 
    state: web::Data<APPState>
) -> impl Responder {
    let mut trees = state.trees.lock().unwrap();
    let tree_name = &query.tree_name;
    if let Some(tree) = trees.get_mut(tree_name) {
        println!("Tree found: {:?}", tree); // This will still require Debug
    } else {
        println!("No tree found with the name: {}", tree_name);
    }
    if let Some(cache) = trees.get_mut(tree_name) {
    if cache.tree.is_none() {
        match load_tree(tree_name) {
            Ok(tree) => {
                cache.tree = Some(tree);
            },
            Err(e) => {
                return HttpResponse::InternalServerError().body(format!("Error loading tree: {}", e));
            }
        }
    }
    cache.last_accessed = Instant::now();
} else {
    println!("Creating new cache entry for {}", tree_name);
    let new_cache = KDTreeCache {
        tree: None, // Initially, there is no tree
        last_accessed: Instant::now(),
    };
    trees.insert(tree_name.to_string(), new_cache);
    match load_tree(tree_name) {
        Ok(tree) => {
            if let Some(cache) = trees.get_mut(tree_name) {
                cache.tree = Some(tree);
            }
        },
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!("Error loading tree: {}", e));
        }
    }
}
if let Some(ref cache) = trees.get(tree_name) {
    if let Some(ref tree) = cache.tree {
        if let Some(n) = query.n {
            if let Some(nearest_neighbors) = tree.nearest_neighbors_topn(&data.into_inner(), n) {
                return HttpResponse::Ok().json(nearest_neighbors);
            }
        }
    }
}


    // Manage memory
    manage_memory(&mut trees, state.max_memory_usage);
    HttpResponse::NotFound().body("No nearest neighbors found or tree not found")
}


// API route to get the status of all KD-Trees
async fn get_status(state: web::Data<APPState>) -> impl Responder {
    let mut trees = state.trees.lock().unwrap(); // Need mutable access to load trees if necessary

    let status: Vec<_> = trees.iter_mut().map(|(tree_name, cache)| {
        // Check if the tree is offloaded (i.e., not in memory)
        if cache.tree.is_none() {
            // Attempt to load the tree from disk
            if let Ok(loaded_tree) = load_tree(tree_name) {
                cache.tree = Some(loaded_tree);
            }
        }

        // After loading, check the length
        json!({
            "tree_name": tree_name,
            "num_records": cache.tree.as_ref().map_or(0, |tree| tree.len()), // Get number of records, or 0 if tree is still None
            "in_memory": cache.tree.is_some(), // Check if the tree is in memory
            "last_accessed": cache.last_accessed.elapsed().as_secs(), // Time in seconds since last accessed
        })
    }).collect();

    HttpResponse::Ok().json(json!({
        "active_trees": status.len(), // Number of active trees
        "trees": status, // Detailed info about each tree
    }))
}


// Similar approach for nearest_neighbor_top_n

#[actix_web::main]
async fn main() -> io::Result<()> {
    let trees: HashMap<String, KDTreeCache> = HashMap::new();


    let shared_data = web::Data::new(APPState {
        trees: Mutex::new(trees),
        max_memory_usage: 1024 * 1024 * 1024, // 1GB of memory limit (adjust as needed)
    });

    let address = "127.0.0.1:8080";
    let server = HttpServer::new(move || {
        App::new()
            .app_data(shared_data.clone())
            .route("/insert", web::post().to(insert_point))
            .route("/nearesttop", web::post().to(nearest_neighbor_top_n))
            .route("/status", web::get().to(get_status))
    })
    .bind(address)?;
    
    println!("Server running on {}", address);
    server.run().await
}
