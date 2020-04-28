// Read CSV
var df_census_csv = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("F:/Data/Census.csv")

// Read JSON
var df_census_json = spark.read.json("F:/Data/Census.json")

// Show the df
df_census_csv.show()
df_census_json.show()


// Writing to PARQUET

// Using CSV Data frame
df_census_csv.write.parquet("F:/Data/Output/census_csv.parquet")

// Using JSON Data frame
df_census_json.write.parquet("F:/Data/Output/census_json.parquet")


// Reading PARQUET file

val df_census_parquet = spark.read.parquet("F:/Data/Output/census_csv.parquet")
df_census_parquet.show()
