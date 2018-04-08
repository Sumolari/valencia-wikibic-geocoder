# About

A simple tool to geocode data in Wikipedia Valencia BIC tables:

- https://es.wikipedia.org/wiki/Anexo:Bienes_de_relevancia_local_de_la_ciudad_de_Valencia_(A-L)
- https://es.wikipedia.org/wiki/Anexo:Bienes_de_relevancia_local_de_la_ciudad_de_Valencia_(M-Z)

This tool generates a CSV with original content and coordinates for those rows without them and a new column with the zipcode.

# Installation

1. Install dependencies: `yarn` or `npm i `
2. Set up API key in `.apprc` (example in `apprc.sample`)
3. Prepare input file in `source.txt` (example in `source.txt.sample`) or `source.csv` (example in `source.csv.sample`)

# Usage

If you use a CSV input file, just run:

- `node index.js `

If you use a txt input file, run:

- `node convertToCSV.js`
- `node index.js`

Each step will generate some cache files:

- `data.cache.json`
- `geocoded.cache.json`
- `reversed.cache.json`

Output file will be available at `output.csv`
