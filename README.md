![Header](./docs/assets/github-header-banner-bord4-data.png)

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
| [`data/raw/region-transitions/subdistrict_adjustments.csv`](data/raw/region-transitions/subdistrict_adjustments.csv) | CSV | Kuraterte grensejusteringer som deler en grunnkrets, og derfor ikke finnes som en hel grunnkretskant i Klass. |

Kolonner i CSV:

| Kolonne | Beskrivelse |
| --- | --- |
| `effective_date` | Dato overgangen trådte i kraft. |
| `from_year` | Året gammel kode representerer. |
| `to_year` | Året ny kode representerer. |
| `level` | `county` eller `municipality`. |
| `from_id` | Gammel SSB-regionkode. |
| `to_id` | Ny SSB-regionkode. |
| `change_type` | `renumbered`, `merge`, `split`, `boundary_change` eller `boundary_adjustment`. |
| `mapping_quality` | `safe_merge`, `split_merge` eller `boundary_adjustment`. |
| `safe_to_aggregate` | `true` når hele den gamle regionen går entydig til én ny region. |
| `event_id` | Felles id for alle kanter i samme sammenhengende endringshendelse. |
| `source_file` | Klass-klassifikasjonen eller den kuraterte råfilen raden er bygget fra. |
| `source_url` | Direkte lenke til kilden. |
| `note` | Merknad om avledning eller kuratert endring, når relevant. |

`boundary_adjustment` er en informasjonskant: bare deler av kommunen flyttet, mens
gammel kommune fortsatte. Den skal ikke følges som etterfølger for hele kommunen.
`split_merge` betyr at den gamle regionen har flere mål, slik at direkte mapping
er tvetydig. Dette gjelder for eksempel begge kantene fra Vågsøy til Kinn og Stad.
Andre kommuner i samme reformhendelse merkes fortsatt `safe_merge` dersom hele
den aktuelle kommunen går entydig til ett mål.

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

Byggescriptet henter kommuneendringer (Klass 131), fylkesendringer (Klass 104)
og grunnkretsendringer (Klass 1) fra SSB fra og med 2000. Grunnkretsene brukes
til å avdekke delinger og grensejusteringer som en ren kommunetabell skjuler.

Når en offisiell kilde beskriver en arealoverføring som ikke finnes i Klass-endepunktet, legges kanten i
`data/raw/region-transitions/subdistrict_adjustments.csv` med kildelenke og
forklaring. Dette omfatter også enkelte grensejusteringer som trådte i kraft
midt i året. De eldre JSON-snapshottene i samme mappe er beholdt som historisk
arbeidsmateriale, men brukes ikke av byggeren.

Bygg dist-filene:

```bash
make data
```

eller:

```bash
python3 scripts/build_region_transitions.py
```

Kontroller at alle kommunekoder som har vært gyldige siden 2000 kan spores til
minst én gjeldende kommune:

```bash
python3 scripts/validate_region_transitions.py
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

- [SSB Klass 131](https://www.ssb.no/klass/klassifikasjoner/131) brukes for kommuneendringer.
- [SSB Klass 104](https://www.ssb.no/klass/klassifikasjoner/104) brukes for fylkesendringer.
- [SSB Klass 1](https://www.ssb.no/klass/klassifikasjoner/1) brukes for endringer i hele grunnkretser.
- SSBs [«Alle endringer i de regionale inndelingene»](https://www.ssb.no/metadata/alle-endringer-i-de-regionale-inndelingene)
  brukes for dokumenterte overføringer av deler av grunnkretser.
- Kommunal- og distriktsdepartementets [oversikt over grensejusteringer](https://www.regjeringen.no/no/tema/kommuner-og-regioner/kommunestruktur/Verktoy/justeringer-av-kommunegrenser/id2571217/)
  brukes for vedtatte justeringer som ikke finnes i Klass-endepunktet.

## Lisens

Dette prosjektet bruker [MIT-lisensen](LICENSE).
