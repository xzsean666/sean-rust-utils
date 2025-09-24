use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::json;

use sean_rust_utils::web2_utils::graphql_helper::GraphQLHelper;

#[derive(Debug, Deserialize, Serialize)]
struct User {
    login: String,
    name: Option<String>,
    email: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Replace with your GraphQL endpoint
    let endpoint = "https://api.github.com/graphql".to_string();

    // Example headers
    let initial_headers = json!({ "Authorization": "Bearer YOUR_GITHUB_TOKEN" });
    let graphql_helper = GraphQLHelper::new(
        endpoint.clone(),
        Some(initial_headers),
    );

    // Query Example
    println!("\n--- Query Example ---");
    let get_user_query = "
        query {
            viewer {
                login
                name
                email
            }
        }
    ".to_string();

    let query_vars = HashMap::new();

    match graphql_helper.query::<HashMap<String, Value>>(get_user_query, Some(query_vars)).await {
        Ok(data) => {
            if let Some(viewer) = data.get("viewer") {
                println!("Raw Viewer Data: {:?}", viewer);
                let user: User = serde_json::from_value(viewer.clone())?;
                println!("User Data: {:?}", user);
            } else {
                println!("No viewer data found.");
            }
        },
        Err(e) => eprintln!("Error fetching viewer: {}", e),
    }

    // Mutation Example (commented out for GitHub API as it's typically for specific repos/user actions)
    /*
    println!("\n--- Mutation Example ---");
    let update_user_mutation = "
        mutation UpdateUser($id: ID!, $name: String!) {
            updateUser(id: $id, name: $name) {
                id
                name
                email
            }
        }
    ".to_string();

    let mut mutation_vars = HashMap::new();
    mutation_vars.insert("id".to_string(), Value::String("123".to_string()));
    mutation_vars.insert("name".to_string(), Value::String("New Name".to_string()));

    match graphql_helper.mutate::<HashMap<String, User>>(update_user_mutation, Some(mutation_vars)).await {
        Ok(data) => {
            if let Some(updated_user) = data.get("updateUser") {
                println!("Updated User: {:?}", updated_user);
            } else {
                println!("No updated user data found.");
            }
        },
        Err(e) => eprintln!("Error updating user: {}", e),
    }
    */

    // Set new headers example (commented out as initial headers are set for GitHub API)
    /*
    println!("\n--- Set Headers Example ---");
    let new_headers_to_set = json!({ "X-Custom-Header": "CustomValue", "Another-Header": "AnotherValue" });
    let mut graphql_helper_mut = GraphQLHelper::new(
        endpoint.clone(),
        Some(json!({ "Authorization": "Bearer initial-token" })),
    );
    graphql_helper_mut.set_headers(new_headers_to_set);
    println!("Headers updated on mutable helper.");
    */

    Ok(())
}
