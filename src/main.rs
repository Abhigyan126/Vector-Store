use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;
use std::io::{self};
use std::time::Instant;
use std::path::{Path, PathBuf};
use std::fs;
use serde_json::json;
use dotenv::dotenv;
use std::env;

mod kdtree;
use kdtree::{KDTree, Point, Node};

struct APPState {
    trees: Mutex<HashMap<String, KDTreeCache>>,
    max_memory_usage: usize,
    bin_directory: PathBuf,
}

#[derive(Debug)]
struct KDTreeCache {
    tree: Option<KDTree>,
    last_accessed: Instant,
}

#[derive(Deserialize)]
struct QueryParams {
    tree_name: String,
    n: Option<usize>,
}

fn ensure_bin_directory(path: &Path) -> io::Result<()> {
    if !path.exists() {
        println!("Creating bin directory at: {:?}", path);
        fs::create_dir_all(path)?;
    }
    Ok(())
}

fn get_bin_file_path(bin_directory: &Path, tree_name: &str) -> PathBuf {
    bin_directory.join(format!("{}.bin", tree_name))
}

fn load_tree(bin_directory: &Path, tree_name: &str) -> io::Result<KDTree> {
    let file_path = get_bin_file_path(bin_directory, tree_name);
    if !file_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("File not found: {:?}", file_path)
        ));
    }
    KDTree::load_from_file(file_path.to_str().unwrap())
}

fn offload_tree(bin_directory: &Path, tree_name: &str, tree: &KDTree) -> io::Result<()> {
    let file_path = get_bin_file_path(bin_directory, tree_name);
    tree.save_to_file(file_path.to_str().unwrap())
}

fn estimate_memory_usage(tree: &KDTree) -> usize {
    let mut total_size = 0;
    total_size += std::mem::size_of::<KDTree>();
    if let Some(root) = &tree.root {
        total_size += estimate_node_size(&root);
    }
    total_size
}

fn estimate_node_size(node: &Box<Node>) -> usize {
    let mut total_size = 0;
    total_size += std::mem::size_of_val(node);
    if let Some(left_child) = &node.left {
        total_size += estimate_node_size(&left_child);
    }
    if let Some(right_child) = &node.right {
        total_size += estimate_node_size(&right_child);
    }
    total_size
}

fn manage_memory(
    trees: &mut HashMap<String, KDTreeCache>,
    max_memory_usage: usize,
    bin_directory: &Path
) {
    let mut total_memory_usage = 0;

    for cache in trees.values() {
        if let Some(tree) = &cache.tree {
            total_memory_usage += estimate_memory_usage(tree);
        }
    }

    while total_memory_usage > max_memory_usage {
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

        if let Some((tree_name, _)) = least_recently_used {
            if let Some(cache) = trees.get_mut(&tree_name) {
                if let Some(tree) = cache.tree.take() {
                    offload_tree(bin_directory, &tree_name, &tree).unwrap();
                    total_memory_usage -= estimate_memory_usage(&tree);
                }
            }
        } else {
            break;
        }
    }
}

async fn insert_point(
    data: web::Json<Point>,
    query: web::Query<QueryParams>,
    state: web::Data<APPState>
) -> impl Responder {
    let mut trees = state.trees.lock().unwrap();
    let tree_name = &query.tree_name;

    // Check if the tree is in memory
    let cache = trees.entry(tree_name.clone()).or_insert_with(|| KDTreeCache {
        tree: None,
        last_accessed: Instant::now(),
    });

    // Try loading from disk if the tree isn't in memory
    if cache.tree.is_none() {
        match load_tree(&state.bin_directory, tree_name) {
            Ok(loaded_tree) => cache.tree = Some(loaded_tree),
            Err(e) => {
                // If loading fails, create a new tree and log the error
                println!("Error loading KD-Tree from file: {}, creating a new one", e);
                cache.tree = Some(KDTree::new(data.0.len()));
            }
        }
    }

    // Update last accessed time
    cache.last_accessed = Instant::now();

    // Insert the new point and attempt to save the updated tree
    if let Some(ref mut tree) = cache.tree {
        tree.insert(data.into_inner());

        // Save the KD-tree to disk
        if let Err(e) = offload_tree(&state.bin_directory, tree_name, tree) {
            return HttpResponse::InternalServerError().body(format!("Failed to save KD-Tree: {}", e));
        }

        // Manage memory if the usage exceeds limits
        manage_memory(&mut trees, state.max_memory_usage, &state.bin_directory);
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

    if let Some(cache) = trees.get_mut(tree_name) {
        if cache.tree.is_none() {
            match load_tree(&state.bin_directory, tree_name) {
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
        let new_cache = KDTreeCache {
            tree: None,
            last_accessed: Instant::now(),
        };
        trees.insert(tree_name.to_string(), new_cache);
        match load_tree(&state.bin_directory, tree_name) {
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

    manage_memory(&mut trees, state.max_memory_usage, &state.bin_directory);
    HttpResponse::NotFound().body("No nearest neighbors found or tree not found")
}

async fn get_status(state: web::Data<APPState>) -> impl Responder {
    let mut trees = state.trees.lock().unwrap();

    let status: Vec<_> = trees.iter_mut().map(|(tree_name, cache)| {
        if cache.tree.is_none() {
            if let Ok(loaded_tree) = load_tree(&state.bin_directory, tree_name) {
                cache.tree = Some(loaded_tree);
            }
        }

        json!({
            "tree_name": tree_name,
            "num_records": cache.tree.as_ref().map_or(0, |tree| tree.len()),
            "in_memory": cache.tree.is_some(),
            "last_accessed": cache.last_accessed.elapsed().as_secs(),
        })
    }).collect();

    HttpResponse::Ok().json(json!({
        "active_trees": status.len(),
        "trees": status,
    }))
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    // Get configuration from environment variables with defaults
    let host = env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let max_memory_mb = env::var("MAX_MEMORY_MB")
        .unwrap_or_else(|_| "1024".to_string())
        .parse::<usize>()
        .unwrap_or(1024);
    let bin_directory = env::var("BIN_DIRECTORY")
        .unwrap_or_else(|_| "bin".to_string());

    // Create bin directory if it doesn't exist
    let bin_path = PathBuf::from(&bin_directory);
    ensure_bin_directory(&bin_path)?;

    let trees: HashMap<String, KDTreeCache> = HashMap::new();
    let shared_data = web::Data::new(APPState {
        trees: Mutex::new(trees),
        max_memory_usage: max_memory_mb * 1024 * 1024, // Convert MB to bytes
        bin_directory: bin_path,
    });

    let address = format!("{}:{}", host, port);
    let server = HttpServer::new(move || {
        App::new()
            .app_data(shared_data.clone())
            .route("/insert", web::post().to(insert_point))
            .route("/nearesttop", web::post().to(nearest_neighbor_top_n))
            .route("/status", web::get().to(get_status))
    })
    .bind(&address)?;

    println!("Server running on {}", address);
    println!("Binary files directory: {:?}", bin_directory);
    println!("Maximum memory usage: {} MB", max_memory_mb);
    
    server.run().await
}
