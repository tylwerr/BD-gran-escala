from src.convert_csv_to_avro import ConvertCsvToAvro
from src.convert_csv_to_parquet import ConvertCsvToParquet


def main():
  print("Running")
  files = [
    "data/EarthquakesChile_2000-2024_1pct.csv",
    "data/EarthquakesChile_2000-2024_10pct.csv",
    "data/EarthquakesChile_2000-2024_25pct.csv",
    "data/EarthquakesChile_2000-2024_50pct.csv",
    "data/EarthquakesChile_2000-2024.csv"
  ]
  for file in files:
    print(file)
    ConvertCsvToAvro().convert(file, "output", "null")
    ConvertCsvToAvro().convert(file, "output", "deflate")
    ConvertCsvToAvro().convert(file, "output", "snappy")
    ConvertCsvToParquet().convert(file, "output", "NONE")
    ConvertCsvToParquet().convert(file, "output", "SNAPPY")
    ConvertCsvToParquet().convert(file, "output", "GZIP")
    ConvertCsvToParquet().convert(file, "output", "LZ4")



if __name__ == "__main__": 
  main()