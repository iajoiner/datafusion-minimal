// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, Result};
use datafusion_expr::WindowUDF;
use datafusion_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, ScalarUDF, TableSource,
};
use datafusion_expr::{planner::ExprPlanner, LogicalPlan};
use datafusion_functions::core::planner::CoreFunctionPlanner;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_optimizer::{
    Analyzer, AnalyzerRule, Optimizer, OptimizerConfig, OptimizerContext, OptimizerRule,
};
use datafusion_sql::{
    planner::{ContextProvider, ParserOptions, SqlToRel},
    sqlparser::{dialect::GenericDialect, parser::Parser},
    TableReference,
};

fn observe_analyzer(_plan: &LogicalPlan, _rule: &dyn AnalyzerRule) {}
fn observe_optimizer(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

fn main() {
    let sql = "select * from tab where b > 1.5;";

    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];
    //println!("{:?}", ast);

    // create a logical query plan
    let context_provider = MyContextProvider::new()
        .with_udaf(sum_udaf())
        .with_udaf(count_udaf())
        .with_expr_planner(Arc::new(CoreFunctionPlanner::default()));
    let parser_options = ParserOptions::new()
        .with_parse_float_as_decimal(true);
    let sql_to_rel = SqlToRel::new_with_options(
        &context_provider,
        parser_options,
    );
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();
    println!("raw plan {plan}");
    let config = OptimizerContext::default().with_skip_failing_rules(false);
    let analyzed_plan = Analyzer::new()
        .execute_and_check(plan.clone(), config.options(), observe_analyzer)
        .unwrap();
    println!("analyzed plan {analyzed_plan}");
    let optimized_plan = Optimizer::new()
        .optimize(analyzed_plan.clone(), &config, observe_optimizer)
        .unwrap();
    println!("optimized plan {optimized_plan:#?}");
}

struct MyContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
    udafs: HashMap<String, Arc<AggregateUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl MyContextProvider {
    fn with_udaf(mut self, udaf: Arc<AggregateUDF>) -> Self {
        self.udafs.insert(udaf.name().to_string(), udaf);
        self
    }

    fn with_expr_planner(mut self, planner: Arc<dyn ExprPlanner>) -> Self {
        self.expr_planners.push(planner);
        self
    }

    fn new() -> Self {
        let mut tables = HashMap::new();
        tables.insert(
            "tab".to_string(),
            create_table_source(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Decimal256(4, 2), false),
                Field::new("c", DataType::Boolean, false),
            ]),
        );
        Self {
            tables,
            options: Default::default(),
            udafs: Default::default(),
            expr_planners: vec![],
        }
    }
}

fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(Arc::new(
        Schema::new_with_metadata(fields, HashMap::new()),
    )))
}

impl ContextProvider for MyContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(Arc::clone(table)),
            _ => plan_err!("Table not found: {}", name.table()),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.udafs.get(name).cloned()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
}
