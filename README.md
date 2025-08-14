# gsuite-pae

A vibe coded Python utility to enrich CSV files containing IP addresses with GeoIP information from a **local MaxMind GeoLite2** or commercial database.
The script inserts a single `geoip` column immediately to the right of the IP address column.

## Features

* Uses local MaxMind **GeoLite2-City.mmdb** (required) and **GeoLite2-ASN.mmdb** (optional).
* Auto-detects the IP address column if not specified.
* Skips private, loopback, link-local, and reserved IPs.
* Outputs compact, filterable values:

  ```
  country_iso|region|city|lat|lon|ASn|org
  ```

  Example:

  ```
  US|Washington|Seattle|47.606200|-122.332100|AS15169|Google LLC
  ```
* Handles large CSVs with optional chunked streaming.

## Requirements

* Python 3.7+
* [pandas](https://pandas.pydata.org/)
* [geoip2](https://pypi.org/project/geoip2/)

Install:

```bash
pip install pandas geoip2
```

Maxmind account, API and city level, and optional ASN database. Using their update tool is recommended.
* [https://github.com/maxmind/geoipupdate/](https://github.com/maxmind/geoipupdate/)


## Usage

Basic:

```bash
python geoip_enrich.py --in input.csv --out output.csv --db /path/GeoLite2-City.mmdb --ip-col ip
```

With ASN data:

```bash
python geoip_enrich.py --in input.csv --out output.csv --db /path/GeoLite2-City.mmdb --asn-db /path/GeoLite2-ASN.mmdb --ip-col ip
```

Large files (chunked processing):

```bash
python geoip_enrich.py --in input.csv --out output.csv --db /path/GeoLite2-City.mmdb --ip-col ip --chunksize 50000
```

## Command-line Arguments

| Option             | Description                                            |
| ------------------ | ------------------------------------------------------ |
| `--in`             | Input CSV path (**required**)                          |
| `--out`            | Output CSV path (default: `<input>.geoip.csv`)         |
| `--db`             | Path to GeoLite2-City.mmdb (**required**)              |
| `--asn-db`         | Path to GeoLite2-ASN.mmdb (optional)                   |
| `--ip-col`         | Name of the IP address column (auto-detect if omitted) |
| `--geoip-col`      | Output column name (default: `geoip`)                  |
| `--chunksize`      | Process in chunks (for large files)                    |
| `--encoding`       | CSV encoding (default: `utf-8`)                        |
| `--sep`            | CSV delimiter (default: `,`)                           |
| `--quotechar`      | CSV quotechar (default: `"`)                           |
| `--na-filter`      | Enable pandas NA parsing                               |
| `--keep-invalid`   | Keep a marker for invalid IPs instead of blank         |
| `--invalid-marker` | Marker string for invalid IPs (default: `invalid_ip`)  |

## Output

* A new CSV with the `geoip` column inserted **immediately to the right** of the IP column.
* Example:

| ip           | geoip | other\_data |               |           |             |         |            |     |
| ------------ | ----- | ----------- | ------------- | --------- | ----------- | ------- | ---------- | --- |
| 8.8.8.8      | US    | California  | Mountain View | 37.386000 | -122.083800 | AS15169 | Google LLC | foo |
| 192.168.0.10 |       | bar         |               |           |             |         |            |     |

## License
