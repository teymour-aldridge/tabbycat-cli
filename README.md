# tabbycat-import

A command-line tool to import data from CSV files to Tabbycat instances (using
the Tabbycat API).

## Installation

I intend to eventually distribute this software via system package managers,
however, currently the only way to use this is to install it from source.

The easiest way to do so is to first
[install Rust](https://www.rust-lang.org/tools/install) and then install this
software from git

```
cargo install --git https://git.sr.ht/~teymour/tabbycat-import
```

## Usage

Example

```
tabbycat-import \
  --institutions-csv data/institutions.csv \
  --judges-csv data/judges.csv \
  --teams-csv data/teams.csv \
  --tabbycat-url "https://sometournament.calicotab.com" \
  --tournament thetournament \
  --api-key yourapikey
```

The format of `institutions.csv`, `teams.csv` and `judges.csv` are quite
particular. They should all match the format as documented below. The files in
the `data` directory of this repository may also be helpful as an example.

- `institutions.csv`
  - Headers: `full_name` (required), `short_code` (required), `region`
              (optional)
  - Example row: "Eidgenössische Technische Hochschule Zürich","ETH Zurich","Europe"

- `judges.csv`
  - Headers: `name` (required), `institution` (optional), `institution_clashes`
              (optional, list of institutions that the judge is clashed with,
               in addition to the institution provided in `institution` - do not
               list institutions twice), `email` (optional), `is_ca` (optional)
               - is the person a member of the adjudication core/
               a chief adjudicator, `is_ia` (optional) - is the person an
               independent adjudicator
  - Example row: TODO

- `teams.csv`
  - Headers: `full_name` (required), `short_name` (optional), `code_name`
              (optional), `institution` (optional,  either the short name or
              long name of the institution should work), `seed` (optional,
              number if used for Tabbycat seeding), `emoji` (optional, emoji
              that should be used as the team)
              and then the speaker attributes
              in the form (for the kth speaker) `speakerk_attr`
              (e.g. `speaker1_name`, `speaker2_name1`), see the
              "speaker headers" below for the headers you can add to each
              speaker
  - Speaker headers:
    - `speaker1_name` (required)
    - `speaker1_categories` (optional, e.g. "esl" - will create if not specified)
    - `speaker1_email` (optional)
    - `speaker1_phone` (optional - genuinely why would you want to put
                                  people's phone numbers into a tab
                                  system)
    - `speaker1_anonymous` (optional - either "true" or "false")
    - `speaker1_code_name` (optional, code name if you are using them)
    - `speaker1_url_key` (optional, key used in private URL: probably don't use
                this and let Tabbycat automatically generate them)
    - `speaker1_gender` (optional, one of "M","F","O")
    - `speaker1_pronoun` (optional)
  - Example row: TODO
