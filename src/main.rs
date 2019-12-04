#[macro_use]
extern crate mysql;

use std::fs::File;
use std::io::prelude::*;
use serde_json;
use serde_json::Value;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use mysql as my;
use std::collections::HashMap;



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


fn transform(policies: Vec<(String, mysql::serde_json::Value)>, table_name: &str, ptype: &str, values: Vec<(String, String)>, table_info: HashMap<String, HashMap<String, String>>) -> String {
    // PROCESS: 
    // 1. figure out policy predicates, fill in necessary values (i.e. UPDATE etc) 
    // 2. evaluate condition variables 
    // 3. check to make sure that the update affects some subset of the policy columns
    let mut cols = Vec::new(); 
    for (col, val) in values.iter() {
        cols.push(col.clone()); 
    }
    
    let mut new_predicates : Vec<String> = Vec::new(); 

    let mut cond_var_stmts = Vec::new(); 

    let mut applicable = false; 
    for (y, policy_array) in policies.iter() {
        match policy_array {
            serde_json::Value::Object(p) => {
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
            
                // add condition variable evaluation to txn string 
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
                                    }
                                }, 
                                _ => panic!("unimplemented")
                            }
                        }
                    }, 
                    _ => panic!("unimplemented")
                }
                

                let mut policy_predicates = Vec::new();  
                // replace cond vars and "UPDATE."s

                match predicates.clone() {
                    serde_json::Value::String(x) => {
                        let mut predicate_components = x.clone().replace("WHERE", ""); 
                        let mut predicate_components = predicate_components.split("AND");
                        let mut predicate_components = predicate_components.collect::<Vec<&str>>();
                        
                        println!("Pred comp {:?}", predicate_components); 
                        for comp in &predicate_components {
                            let mut updated = false; 
                            for (cond_var_name, cond_var_statement) in cond_var_stmts.clone() {
                                if comp.contains(&cond_var_name) {
                                    let mut s = format!("@{:?}", cond_var_name);
                                    let cond_var_slice: &str = &*s;  // take a full slice of the string
                                    let cond_var_name_slice = &*cond_var_name; 
                                    let new_stmt = comp.replace(cond_var_name_slice, cond_var_slice);
                                    let new_stmt = new_stmt.trim(); 
                                    let mut new_stmt = new_stmt.replace("\"", "");
                                    let mut new_stmt_mod = new_stmt.split("="); 
                                    let mut new_stmt_mod = new_stmt_mod.collect::<Vec<&str>>()[1];

                                    println!("pol pred {:?}", new_stmt);
                                    policy_predicates.push(new_stmt.to_owned()); 
                                    updated = true; 
                                    break; 
                                }
                            }
                            if updated == false {
                                let mut c = comp.to_string(); 
                                let c = c.trim();
                                policy_predicates.push(c.to_owned()); 
                            }
                        }                    
                    }, 
                    _ => {}, 
                }
                
                for pred in &policy_predicates {
                    // TODO: add quotations based on type 
                    if pred.contains(&"UPDATE") {
                        let mut field = pred.split("UPDATE."); 
                        let mut field = field.collect::<Vec<&str>>()[1];
                        let mut field = field.split("="); 
                        let mut field = field.collect::<Vec<&str>>();
                        let mut updated = false; 
                        let mut tinfo = table_info[table_name].clone(); 
                        for (col, val) in values.iter() {
                            let mut stripped = field[0].replace(" ", "");
                            let mut stripped = field[0].replace(" ", "");  
                            if stripped == *col {
                                let mut to_replace = format!("UPDATE.{}", field[0].trim()); 
                                let mut val = val.clone(); 
                                if tinfo[col] == "str" {
                                    println!("tinfo col {}", tinfo[col]); 
                                    val = format!("'{}'", val); 
                                }
                                let mut new_pred = pred.replace(&*to_replace, &*val);
                                let mut new_pred = new_pred.trim();  
                                updated = true; 
                                new_predicates.push(new_pred.to_string()); 
                            }
                        }
                        if !updated {
                            new_predicates.push(pred.to_string()); 
                        }
                    } else {
                        new_predicates.push(pred.to_string()); 
                    }
                }


            }, 
            _ => panic!("unimplemented")
        }
    }

    let mut txn = "START TRANSACTION; ".to_string(); 
    for (var, cmd) in cond_var_stmts.iter() {
        txn = format!("{} {}; ", txn, cmd); 
    }

    if ptype == "insert" {
        txn = format!("{}{}", txn, format!("INSERT INTO {} (", table_name)); 

        let mut i = 0;
        for (col, val) in values.iter() {
            if i == 0 {
                txn = format!("{} {}", txn, &*col.trim());
            } else {
                txn = format!("{}, {}", txn, &*col.trim());
            }
            i += 1;
        }
        
        txn = format!("{}) ", txn); 
        txn = format!("{}{}", txn, "SELECT "); 


        // TODO: add quotes based on type 
        let mut i = 0;
        println!("tinfo {:?}", table_info); 
        let mut tinfo = table_info[table_name].clone(); 
        println!("vals {:?}", values); 
        for (col, val) in values.iter() {
            println!("col {} info {}", col, tinfo[col]); 
            if tinfo[col] == "str" {
                if i == 0 {
                    txn = format!("{} '{}'", txn, &*val.trim());
                } else {
                    txn = format!("{}, '{}'", txn, &*val.trim());
                }
            } else {
                if i == 0 {
                    txn = format!("{} {}", txn, &*val.trim());
                } else {
                    txn = format!("{}, {}", txn, &*val.trim());
                }
            }
            println!("txn : {:?}", txn); 
            i += 1;
        }

        txn = format!("{} WHERE ", txn);
        
        // add predicates 
        let mut i = 0;
        for pred in new_predicates.iter() {
            if i == 0 {
                txn = format!("{} {}", txn, pred);
            } else {
                txn = format!("{} AND {}", txn, pred);
            }
            i += 1; 
        }
        txn = format!("{} {}", txn, "; COMMIT;");
    }

    return txn; 
}


fn translate(updates: &str, policies: serde_json::Map<String, serde_json::Value>, table_info: HashMap<String, HashMap<String, String>>) -> String {
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
            let mut compliant_query = transform(applicable, table_name, ptype, cv_pairs, table_info); 
            return compliant_query; 

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

    let mut table_info = HashMap::new(); 
    let mut col_dict = HashMap::new(); 
    col_dict.insert("pid".to_string(), "int".to_string()); 
    col_dict.insert("name".to_string(), "str".to_string());
    col_dict.insert("role".to_string(), "str".to_string()); 

    table_info.insert("People".to_string(), col_dict); 
    
    // spin up DB & populate!
    let pool = my::Pool::new("mysql://root@localhost:3306/mysql").unwrap();

    pool.prep_exec(r"CREATE TEMPORARY TABLE People (
        pid int not null,
        name text not null,
        role text not null
    );", ()).unwrap();

    pool.prep_exec(r"CREATE TEMPORARY TABLE Comments (
        cid int not null,
        pid int not null,
        comment text not null
    );", ()).unwrap();

    pool.prep_exec(r"CREATE TEMPORARY TABLE Reviewers (
        pid int not null,
        sid int not null);", ()).unwrap();

    pool.prep_exec(r"CREATE TEMPORARY TABLE ConfMeta (
        phase text not null
    );", ()).unwrap();

    pool.prep_exec(r"CREATE TEMPORARY TABLE Submissions (
        sid int not null,
        primary_author text not null,
        title text not null
    );", ()).unwrap();

    pool.prep_exec(r"CREATE TEMPORARY TABLE Reviewers (
        pid int not null,
        sid int not null
    );", ()).unwrap();

    
    for mut stmt in pool.prepare(r"INSERT INTO ConfMeta
                                       (phase)
                                   VALUES
                                       (:phase)").into_iter() {
        stmt.execute(params!{
                        "phase" => "submission",
                    }).unwrap();
    }

    let mut txn = translate(&updates, policy_config, table_info);

    println!("\n\n{}\n\n", txn); 
    pool.prep_exec(txn, ()).unwrap();

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
