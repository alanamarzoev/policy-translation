#[macro_use]
extern crate mysql;

use std::fs::File;
use std::io::prelude::*;
use serde_json;
use serde_json::Value;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use mysql as my;


#[derive(Debug, PartialEq, Eq)]
struct Reviewers {
    pid: i32, // person ID
    sid: i32, // submission ID
}

#[derive(Debug, PartialEq, Eq)]
struct Submissions {
    sid: i32,
    primary_author: String,
    title: String,
}

#[derive(Debug, PartialEq, Eq)]
struct Comments {
    cid: i32, // comment id 
    pid: i32, // person id
    comment: String,
}

#[derive(Debug, PartialEq, Eq)]
struct People {
    pid: i32,
    name: String,
    role: String,
}

fn get_applicable(table_name: &str, policy_type: &str, 
                  all_policies: serde_json::Map<String, serde_json::Value>) -> Vec<(String, mysql::serde_json::Value)> {   
    let mut policy_array = &all_policies["policies"]; 
    let mut applicable = Vec::new(); 
    match policy_array {
        serde_json::Value::Array(p) => {
            for policy in p.iter() {
                match policy["columns"].clone() {
                    serde_json::Value::String(x) => {
                        if x.contains(&table_name) {
                            applicable.push((table_name.to_string().clone(), policy.clone())); 
                        }
                    }, 
                    _ => panic!("unimplemented")
                }
            }
        }, 
        _ => panic!("unimplemented")
    }
    return applicable
}

fn transform(policies: Vec<(String, mysql::serde_json::Value)>, table_name: &str, ptype: &str, values: Vec<(String, String)>) {
    // PROCESS: 
    // 1. figure out policy predicates, fill in necessary values (i.e. UPDATE etc) 
    // 2. evaluate condition variables 
    // 3. check to make sure that the update affects some subset of the policy columns
    let mut cols = Vec::new(); 
    for (col, val) in values.iter() {
        cols.push(col.clone()); 
    }
    
    let mut txn = r"BEGIN TRANSACTION "; 
    let mut applicable = false; 
    for (y, policy_array) in policies.iter() {
        match policy_array {
            serde_json::Value::Object(p) => {
                println!("p: {:?}", p["condition_vars"]); 
                let mut condition_vars = &p["condition_vars"]; 
                let mut columns = &p["columns"]; 
                let mut predicates = &p["predicate"]; 
                let mut policy_type = &p["type"]; 

                // make sure the update affects some subset of the policy cols 
                match columns.clone() {
                    serde_json::Value::String(x) => {
                        if x.contains("*") {
                            applicable = true; 
                        }
                        for col in &cols {
                            if x.contains(col) {
                                applicable = true; 
                            }
                        }
                    }, 
                    _ => panic!("unimplemented")
                }
            
                let mut cond_var_stmts = Vec::new(); 
                // add condition variable evaluation to txn string 
                println!("condition_vars: {:?}", condition_vars);
                match condition_vars.clone() {
                    serde_json::Value::Array(p) => {
                        for predicate in p.iter() {
                            match predicate {
                                serde_json::Value::Object(x) => {
                                    for (cond_var_name, predicate) in x.iter() {
                                        let pred = &x[cond_var_name];
                                        match pred.clone() {
                                            serde_json::Value::String(h) => {
                                                cond_var_stmts.push((cond_var_name.clone(), h.clone())); 
                                            }, 
                                            _ => panic!("unimplemented")
                                        }
                                        println!("h");
                                        println!("{:?}", pred);
                                    }
                                }, 
                                _ => panic!("unimplemented")
                            }
                        }
                    }, 
                    _ => panic!("unimplemented")
                }
                
                match predicates.clone() {
                    serde_json::Value::String(x) => {
                        let cleaned = x.clone().replace(&['(', ')', ',', '\"', ';', ':', '\'', '\n'][..], "");
                        for (cond_var_name, cond_var_statement) in cond_var_stmts {
                            if cleaned.contains(&cond_var_name) {
                                println!("found cond var name: {:?} in cleaned: {:?}", cond_var_name, cleaned); 
                            }
                        }
                    }, 
                    _ => {}, 
                }

            }, 
            _ => panic!("unimplemented")
        }
    }
    

    for (col, val) in values.iter() {
        
    }

    println!("p: {:#?}", policies); 
    // let mut query_str_0 = format!(r"INSERT INTO {:?} ({:?}", table_name, col); 
    // let mut query_str_1 = r" VALUES ( "; 
    // for (col, val) in values.iter() { 
        
    //     let mut new_0 = format!("{:?} {:?}", query_str_0, col); 
    //     let mut new_1 = format!("{:?}")
        
    // }
    // r"INSERT INTO payment
    //                                    (customer_id, amount, account_name)
    //                                VALUES
    //                                    (:customer_id, :amount, :account_name)"
}


fn translate(updates: &str, policies: serde_json::Map<String, serde_json::Value>) {
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let query = Parser::parse_sql(&dialect, updates.to_string()).unwrap();

    let mut split_str = updates.split("(");
    let mut values: Vec<String>= Vec::new();
    
    let mut i = 0;
    for s in split_str {
        if i == 2 {
            let m = s.clone().replace(&['(', ')', ',', '\"', '.', ';', ':', '\'', '\n'][..], "");
            let vals = m.split(" "); 
            for val in vals {
                println!("split: {:#?}", val.clone());
                values.push(val.to_owned()); 
            }
        }
        i += 1; 
    }

    match query[0].clone() {
        sqlparser::ast::Statement::Insert{table_name, columns, source} => {
            let ptype = "insert";
            let table_name = &table_name.0[0];
            let mut i = 0;
            let mut cv_pairs = Vec::new();
            for column in &columns {
                cv_pairs.push((column.clone(), values[i].clone()));
                i += 1; 
            }
            let mut applicable = get_applicable(table_name, ptype, policies.clone());
            let mut compliant_query = transform(applicable, table_name, ptype, cv_pairs); 

        }, 
        sqlparser::ast::Statement::Update{table_name, assignments, selection} => {
            panic!("unimplemented");
            // let ptype = "update"; 
            // let table_name = &table_name.0[0];
            // let mut i = 0;
            // let mut cv_pairs = Vec::new();
            // for column in &columns {
            //     cv_pairs.push((column.clone(), values[i].clone()));
            //     i += 1; 
            // }
            // let mut applicable = get_applicable(table_name, ptype, policies);
            // let mut compliant_query = transform(table_name, ptype, cv_pairs); 

        },
        sqlparser::ast::Statement::Delete{table_name, selection} => {
            panic!("unimplemented");
            // let ptype = "delete"; 
            // let table_name = &table_name.0[0];
            // let mut i = 0;
            // let mut cv_pairs = Vec::new();
            // for column in &columns {
            //     cv_pairs.push((column.clone(), values[i].clone()));
            //     i += 1; 
            // }
            // let mut applicable = get_applicable(table_name, ptype, policies);
            // let mut compliant_query = transform(table_name, ptype, cv_pairs); 

        }, 
        _ => panic!("unimplemented!")
    }
}

fn bootstrap(updates_path: &str, policy_path: &str) -> std::io::Result<()> {
    // read in policy + update files ... 
    let mut policy_file = File::open(policy_path).unwrap();
    let mut updates_file = File::open(updates_path).unwrap();

    let mut policies = String::new();
    let mut updates = String::new(); 

    policy_file.read_to_string(&mut policies);
    updates_file.read_to_string(&mut updates);
    
    let policy_config: serde_json::Map<String, Value> = serde_json::from_str(&policies)?;
    
    // spin up DB & populate!
    // let pool = my::Pool::new("mysql://root@localhost:3306/mysql").unwrap();

    // pool.prep_exec(r"CREATE TEMPORARY TABLE People (
    //     pid int not null,
    //     name text not null,
    //     role text not null
    // )", ()).unwrap();

    // pool.prep_exec(r"CREATE TEMPORARY TABLE Comments (
    //     cid int not null,
    //     pid int not null,
    //     comment text not null
    // )", ()).unwrap();

    // pool.prep_exec(r"CREATE TEMPORARY TABLE Reviewers (
    //     pid int not null,
    //     sid int not null    // )", ()).unwrap();

    // pool.prep_exec(r"CREATE TEMPORARY TABLE ConfMeta (
    //     phase text not null
    // )", ()).unwrap();

    // pool.prep_exec(r"CREATE TEMPORARY TABLE Submissions (
    //     sid int not null,
    //     primary_author text not null,
    //     title text not null
    // )", ()).unwrap();

    // pool.prep_exec(r"CREATE TEMPORARY TABLE Reviewers (
    //     pid int not null,
    //     sid int not null
    // )", ()).unwrap();

    
    // for mut stmt in pool.prepare(r"INSERT INTO ConfMeta
    //                                    (phase)
    //                                VALUES
    //                                    (:phase)").into_iter() {
    //     stmt.execute(params!{
    //                     "phase" => "submission",
    //                 }).unwrap();
    // }

    translate(&updates, policy_config);

    // let payments = vec![
    //     Payment { customer_id: 1, amount: 2, account_name: None },
    //     Payment { customer_id: 3, amount: 4, account_name: Some("foo".into()) },
    //     Payment { customer_id: 5, amount: 6, account_name: None },
    //     Payment { customer_id: 7, amount: 8, account_name: None },
    //     Payment { customer_id: 9, amount: 10, account_name: Some("bar".into()) },
    // ];

    // for mut stmt in pool.prepare(r"INSERT INTO payment
    //                                    (customer_id, amount, account_name)
    //                                VALUES
    //                                    (:customer_id, :amount, :account_name)").into_iter() {
    //     for p in payments.iter() {
    //         // `execute` takes ownership of `params` so we pass account name by reference.
    //         // Unwrap each result just to make sure no errors happened.
    //         stmt.execute(params!{
    //             "customer_id" => p.customer_id,
    //             "amount" => p.amount,
    //             "account_name" => &p.account_name,
    //         }).unwrap();
    //     }
    // }

    Ok(()) 
}

// read in write policies and list of updates, translate updates to be policy compliant & print 
fn main() {
    use clap::{App, Arg};
    let args = App::new("translation")
        .version("0.1")
        .arg(
            Arg::with_name("updates")
                .short("u")
                .required(true)
                .default_value("src/updates.sql")
                .help("Query file for Piazza application"),
        )
        .arg(
            Arg::with_name("policies")
                .long("policies")
                .required(true)
                .default_value("src/hotcrp-policies.json")
                .help("Security policies file for Piazza application"),
        )
        .get_matches();

    println!("Starting benchmark...");

    // Read arguments
    let ploc = args.value_of("policies").unwrap();
    let uloc = args.value_of("updates").unwrap();
    bootstrap(uloc, ploc);
}
