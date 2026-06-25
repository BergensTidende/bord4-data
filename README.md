# Bord4 data

Kuraterte datasett fra Bord4/BT som er nyttige på tvers av analyser eller som bør kunne deles med lesere.

Repoet har to typer innhold:

- **Fellesdata**: små, vedlikeholdte datasett vi ofte trenger, for eksempel historiske overganger mellom norske kommuner og fylker.
- **Delte saksdatasett**: bearbeidede datasett fra enkeltsaker som vi ønsker å publisere med dokumentasjon.

Folketall og andre ferske offisielle tall bør normalt hentes direkte fra SSB i analysen som bruker dem. Dette repoet skal først og fremst inneholde data som er kuratert, bearbeidet eller vanskelig å hente i en analysevennlig form.

## Datasett

### Norske kommuner

Lett oppslagstabell for gjeldende kommuner, egnet for å mappe mellom kommunenummer, kommunenavn, fylkesnummer og fylkesnavn.

| Fil | Format | Beskrivelse |
| --- | --- | --- |
| [`data/dist/norwegian_municipalities.csv`](data/dist/norwegian_municipalities.csv) | CSV | Gjeldende kommuner per 2026-01-01. |

Kolonner:

| Kolonne | Beskrivelse |
| --- | --- |
| `kommunenummer` | Kommunenummer. |
| `kommunenavn` | Kommunenavn. Der SSB har parallelle offisielle navn, brukes første navn. |
| `fylkesnummer` | Fylkesnummer, avledet fra kommunenummer. |
| `fylkesnavn` | Fylkesnavn. Der SSB har parallelle offisielle navn, brukes første navn. |
| `siste_endret` | Siste gyldighetsstart for gjeldende kommune eller fylke, beregnet fra SSB Klass. |

Eksempel:

```python
import pandas as pd

municipalities = pd.read_csv("data/dist/norwegian_municipalities.csv", dtype=str)
```

### Norsk regionhistorikk

Historisk oppslagstabell for kommuner og fylker tilbake til 1838, basert på SSB Klass. Denne er nyttig når kildedata bruker gamle kommunenummer eller gamle fylker som valgdistrikt, for eksempel `1201` for Bergen i Hordaland.

| Fil | Format | Beskrivelse |
| --- | --- | --- |
| [`data/dist/norwegian_regions_history.csv`](data/dist/norwegian_regions_history.csv) | CSV | Historiske kommuner og fylker med gyldighetsperiode. |

Kolonner:

| Kolonne | Beskrivelse |
| --- | --- |
| `nivå` | `kommune` eller `fylke`. |
| `regionnummer` | Kommune- eller fylkesnummer. |
| `regionnavn` | Kommune- eller fylkesnavn. Der SSB har parallelle offisielle navn, brukes første navn. |
| `fylkesnummer` | Fylkesnummer for raden. For fylkesrader er dette samme som `regionnummer`. |
| `fylkesnavn` | Fylkesnavn for raden. For fylkesrader er dette samme som `regionnavn`. |
| `gyldig_fra` | Første dato regionnavn/kode er gyldig i SSB Klass. |
| `gyldig_til` | Siste dato regionnavn/kode er gyldig i SSB Klass. Tomt felt betyr gjeldende. |
| `er_gjeldende` | `true` eller `false`. |

Eksempel:

```python
import pandas as pd

history = pd.read_csv("data/dist/norwegian_regions_history.csv", dtype=str)
bergen_1201 = history.query("nivå == 'kommune' and regionnummer == '1201'")
```

### Norske regionoverganger

Maskinlesbare overganger mellom norske kommuner og fylker.

| Fil | Format | Beskrivelse |
| --- | --- | --- |
| [`data/dist/norwegian_region_transitions.csv`](data/dist/norwegian_region_transitions.csv) | CSV | Flat overgangstabell, én rad per kant fra gammel til ny regionkode. |
| [`data/dist/norwegian_region_transitions.json`](data/dist/norwegian_region_transitions.json) | JSON | Samme innhold som CSV, egnet for Node/JavaScript. |
| [`data/raw/region-transitions/`](data/raw/region-transitions/) | JSON | Rå overgangsmappinger som dist-filene bygges fra. |

Kolonner i CSV:

| Kolonne | Beskrivelse |
| --- | --- |
| `from_year` | Året gammel kode representerer. |
| `to_year` | Året ny kode representerer. |
| `level` | `county` eller `municipality`. |
| `from_id` | Gammel SSB-regionkode. |
| `to_id` | Ny SSB-regionkode. |
| `change_type` | `renumbered`, `merge`, `split` eller `boundary_change`. |
| `source_file` | Råfilen raden er bygget fra. |

Eksempel:

```python
import pandas as pd

transitions = pd.read_csv("data/dist/norwegian_region_transitions.csv", dtype=str)
```

```js
import transitions from "./data/dist/norwegian_region_transitions.json" assert { type: "json" };
```

## Delte saksdatasett

Legg datasett fra enkeltsaker i `data/shared/<saksslug>/`.

Hver mappe bør inneholde:

- selve datasettet i CSV eller JSON
- en kort `README.md` med kilde, metode, kontaktpunkt og publiseringsdato
- scripts/notebooks bare hvis de er nødvendige for å forstå eller reprodusere datasettet

## Oppdatere regionoverganger

Rå overgangsfiler legges i `data/raw/region-transitions/` som JSON:

```json
["2024", "2025", {
  "31": ["30"]
}]
```

To former støttes:

- `{"gammel_kode": "ny_kode"}` for enkle renummereringer og sammenslåinger
- `{"ny_kode": ["gammel_kode"]}` for splittinger

Bygg dist-filene:

```bash
make data
```

eller:

```bash
python3 scripts/build_region_transitions.py
```

Kommunetabellen bygges fra SSB Klass:

```bash
python3 scripts/build_current_municipalities.py
```

Regionhistorikken bygges fra SSB Klass:

```bash
python3 scripts/build_regions_history.py
```

## Kilder

- SSB Klass brukes som autoritativ kilde for kommune- og fylkeskoder.
- Kartverket og valgrelaterte arbeidsfiler brukes som grunnlag for historiske overganger der SSB/Kartverket ikke gir én analyseferdig overgangstabell.

## Lisens

Dette prosjektet bruker [MIT-lisensen](LICENSE).
