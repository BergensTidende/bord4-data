# Bord4 data
Collection of datasets and map files used by data journalists at [Bergens Tidendes](https://www.bt.no).

Data files are stored in the data folder. Scripts for generating the datasets are found in the script folder.

## Data files

### Datasets
| Name | Download | Preview | Script |
| --- | :---: | :---: | :--: |
| Norwegian regions | [csv](data/csv/norwegian_regions.csv) [pickle](data/csv/norwegian_regions.csv) | [data/csv/norwegian_regions.csv](data/csv/norwegian_regions.csv) | script |
| Norwegian regions - changes 2019/2020 | [csv](data/csv/norwegian_regions.csv) [pickle](data/csv/norwegian_regions.csv) | [data/csv/norwegian_regions.csv](data/csv/norwegian_regions.csv) | script |
| Population for Norwegian regions | [csv](data/csv/norwegian_regions.csv) [pickle](data/csv/norwegian_regions.csv) | [data/csv/norwegian_regions.csv](data/csv/norwegian_regions.csv) | script |
| Population 2020 for Norwegian regions | [csv](data/csv/norwegian_regions.csv) [pickle](data/csv/norwegian_regions.csv) | [data/csv/norwegian_regions.csv](data/csv/norwegian_regions.csv) | script |

### Map files
| Name | Download | Preview |
| --- | :---: | :---: |

### Example
To get a dataframe of all norwegian regions do the following

```
df = pd.read_pickle(<link_to_data.pkl>)
```
or
```
df = pd.read_csv(<link_to_data.pkl>)
```

## Sources

### Population
* Source: [https://www.ssb.no/statbank/table/10826/] [https://www.ssb.no/statbank/table/07459/]
* Last updated: 2020-07-29
* Note: For cities there's only population for 2020 for the moment.

### Regions
* Source: [https://www.ssb.no/statbank/table/10826/] [https://www.ssb.no/statbank/table/07459/]
* Last updated: 2020-07-29
* Note: Generated from the population files

### Regional changes
* Source: [https://www.kartverket.no/kommunereform/tekniske-endringer/kommune--og-regionsendringer-2020/]
* Last updated: 2020-07-29

## Updating the data
To update the data you must first run

```bash
pipenv install
```

To run just one of the script run

```bash
pipenv run python script/<name>.py
```

To generate all the data files run

```bash
make data
```

## The team

Bord4 is the data jouranslits and news deveopers at Bergens Tidende. Bord is Norwegian for table, so it translates to table 4. Many aeons ago bord4 sat around the fourth table in a corridor.

* [Tove B. Knutsen](https://twitter.com/tovek)
* [Lasse Lambrechts](https://twitter.com/lambrechts)
* [Anders Grimsrud Eriksen ](https://twitter.com/anderser)
* [Philipp Julius Bock](https://twitter.com/bockph)
* [Halvard Alvheim Vegum](https://twitter.com/Havegum)

## Contact

If you want to contact us you can reach us at <bord4@bt.no>.

## License
This project uses the following license: [MIT](LICENSE).
