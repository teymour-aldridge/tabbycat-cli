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
cargo install --git https://github.com/teymour-aldridge/tabbycat-import
```

## Usage

Note: running `tabbycat --help` will print useful information about comamands.

### Importing teams

Example

```
tabbycat import \
  --institutions-csv data/institutions.csv \
  --judges-csv data/judges.csv \
  --teams-csv data/teams.csv \
  --clashes-csv data/clashes.csv \
  --tabbycat-url "https://sometournament.calicotab.com" \
  --tournament thetournament \
  --api-key yourapikey
```

The format of `institutions.csv`, `teams.csv`, `judges.csv` and `clashes.csv`
is quite particular. They should all match the format as documented below. The
files in the `data` directory of this repository may also be helpful as an
example.

- `institutions.csv`
  - Headers: `full_name` (required), `short_code` (required), `region`
    (optional)
  - Example row: "Eidgenössische Technische Hochschule Zürich","ETH Zurich","Europe"

- `judges.csv`
  - Headers: `name` (required), `institution` (optional), `institution_clashes`
    (optional, list of institutions that the judge is clashed with,
    in addition to the institution provided in `institution` - do not
    list institutions twice), `email` (optional), `is_ca` (optional) - is the person a member of the adjudication core/
    a chief adjudicator, `is_ia` (optional) - is the person an
    independent adjudicator
  - Example row: TODO

- `teams.csv`
  - Headers: `full_name` (required), `short_name` (optional), `code_name`
    (optional), `institution` (optional, either the short name or
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

- `clashes.csv`
  - Headers: none. Each CSV file should have two columns. Each column should
    contain the name of an entity which should be clashed. Clashing is a
    symmetric relation (that is, the order in which clashes are listed doesn't
    matter).
  - Example rows.
    - To clash two people, just enter them on a row, for example:
      - To clash a speaker from a team against an adjudicator

        ```
        speaker name,adjudicator name
        ```

        or

        ```
        adjudicator name,speaker name
        ```

      - To clash an adjudicator against another adjudicator
        ```
        adjudicator name,other adjudicator name
        ```
      - To clash an adjudicator against an institution
        ```
        adjudicator_name,institution_name
        ```
      - To clash a team against an institution
        ```
        institution_name,adjudicator_name
        ```
