use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{self};
use bincode;
use std::cmp::Ordering;

// Struct to hold the embedding and associated data
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Point {
    pub embedding: Vec<f64>, // Embedding vector
    pub data: String,        // Associated data (chunk)
}

impl Point {
    // Custom method to get the length of the data
    pub fn len(&self) -> usize {
        self.embedding.len()
    }
}

// KD-Tree Node
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    point: Point,
    pub left: Option<Box<Node>>,
    pub right: Option<Box<Node>>,
    axis: usize,
}

// KD-Tree structure
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KDTree {
    pub root: Option<Box<Node>>,
    k: usize,  // Number of dimensions
}

impl KDTree {
    pub fn new(k: usize) -> Self {
        KDTree { root: None, k }
    }

    pub fn insert(&mut self, point: Point) {
        self.root = KDTree::insert_recursive(self.root.take(), point, 0, self.k);
//        self.save_to_file("kd_tree.bin").unwrap();
    }

    fn insert_recursive(
        node: Option<Box<Node>>,
        point: Point,
        depth: usize,
        k: usize,
    ) -> Option<Box<Node>> {
        if let Some(mut current_node) = node {
            let axis = depth % k;
            if axis >= point.embedding.len() {
                panic!("Axis {} is out of bounds for embedding length {}", axis, point.embedding.len());
            }
            if point.embedding[axis] < current_node.point.embedding[axis] {
                current_node.left = KDTree::insert_recursive(current_node.left.take(), point, depth + 1, k);
            } else {
                current_node.right = KDTree::insert_recursive(current_node.right.take(), point, depth + 1, k);
            }
            Some(current_node)
        } else {
            Some(Box::new(Node {
                point,
                left: None,
                right: None,
                axis: depth % k,
            }))
        }
    }

    pub fn save_to_file(&self, filename: &str) -> Result<(), io::Error> {
        let file = File::create(filename)?;
        bincode::serialize_into(file, self).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn load_from_file(filename: &str) -> Result<Self, io::Error> {
        let file = File::open(filename)?;
        let tree: KDTree = bincode::deserialize_from(file).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(tree)
    }

    pub fn nearest_neighbors_topn<'a>(&'a self, target: &Point, n: usize) -> Option<Vec<&'a Point>> {
        let mut results: Vec<(f64, &'a Point)> = Vec::new();
        self.nearest_recursive_n(&self.root, target, 0, self.k, &mut results); // Assuming this function populates `results`
    
        // Sort results based on distance
        results.sort_by(|(dist_a, _), (dist_b, _)| dist_a.partial_cmp(dist_b).unwrap_or(Ordering::Equal));
    
        // Collect top N points
        let top_n_points: Vec<&'a Point> = results.into_iter().take(n).map(|(_, point)| point).collect();
    
        // Return the top N points if there are any, otherwise return None
        if top_n_points.is_empty() {
            None
        } else {
            Some(top_n_points)
        }
    }
    
    
    fn nearest_recursive_n<'a>(
        &'a self,
        node: &'a Option<Box<Node>>, // Node reference
        target: &Point,              // Target point
        depth: usize,                // Current depth in the tree
        k: usize,                    // Dimensionality
        results: &mut Vec<(f64, &'a Point)>, // Results to collect distances and points
    ) {
        if let Some(current_node) = node {
            let axis = depth % k; // Determine axis based on depth
            let current_point = &current_node.point;
            let dist = euclidean_distance(&current_point.embedding, &target.embedding); // Calculate distance
    
            // Add the current point and its distance to results
            results.push((dist, current_point));
    
            // Determine which branch to explore next
            let (next_branch, other_branch) = if target.embedding[axis] < current_point.embedding[axis] {
                (&current_node.left, &current_node.right)
            } else {
                (&current_node.right, &current_node.left)
            };
    
            // Recursively search the next branch
            self.nearest_recursive_n(next_branch, target, depth + 1, k, results);
    
            // Check if we need to explore the other branch
            if (target.embedding[axis] - current_point.embedding[axis]).abs() < 
                results.iter().map(|(d, _)| *d).fold(f64::INFINITY, f64::min) {
                self.nearest_recursive_n(other_branch, target, depth + 1, k, results);
            }
        }
    }

    //Nearest top

    pub fn nearest_neighbor<'a>(&'a self, target: &Point) -> Option<&'a Point> {
        let mut best: Option<&Point> = None;
        let mut best_distance = f64::INFINITY;
        self.nearest_recursive(&self.root, target, 0, self.k, &mut best, &mut best_distance);
        best
    }

    fn nearest_recursive<'a>(
        &'a self,
        node: &'a Option<Box<Node>>,
        target: &Point,
        depth: usize,
        k: usize,
        best: &mut Option<&'a Point>,
        best_distance: &mut f64,
    ) {
        if let Some(current_node) = node {
            let axis = depth % k;
            let current_point = &current_node.point;
            let dist = euclidean_distance(&current_point.embedding, &target.embedding);

            if dist < *best_distance {
                *best = Some(current_point);
                *best_distance = dist;
            }

            let (next_branch, other_branch) = if target.embedding[axis] < current_point.embedding[axis] {
                (&current_node.left, &current_node.right)
            } else {
                (&current_node.right, &current_node.left)
            };

            self.nearest_recursive(next_branch, target, depth + 1, k, best, best_distance);

            if (target.embedding[axis] - current_point.embedding[axis]).abs() < *best_distance {
                self.nearest_recursive(other_branch, target, depth + 1, k, best, best_distance);
            }
        }
    }

    pub fn len(&self) -> usize {
        // Call a recursive helper function starting from the root
        self.count_nodes(&self.root)
    }

    fn count_nodes(&self, node: &Option<Box<Node>>) -> usize {
        if let Some(ref current_node) = node {
            // Recursively count nodes in the left and right subtrees
            1 + self.count_nodes(&current_node.left) + self.count_nodes(&current_node.right)
        } else {
            0
        }
    }


}



// Function to calculate Euclidean distance
pub fn euclidean_distance(a: &Vec<f64>, b: &Vec<f64>) -> f64 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f64>()
        .sqrt()
}
