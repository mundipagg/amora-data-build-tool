# CLI


```
Usage: amora [OPTIONS] COMMAND [ARGS]...

  Amora Data Build Tool enables engineers to transform data in their
  warehouses by defining schemas and writing select statements with
  SQLAlchemy. Amora handles turning these select statements into tables and
  views

Options:
  --install-completion  Install completion for the current shell.
  --show-completion     Show completion for the current shell, to copy it or
                        customize the installation.
  --help                Show this message and exit.

Commands:
  compile      Generates executable SQL from model files.
  materialize  Executes the compiled SQL against the current target...
  models
  test         Runs tests on data in deployed models.


```

## amora compile

::: amora.cli.compile

## amora materialize

::: amora.cli.materialize

## amora test

::: amora.cli.test

## amora models list

::: amora.cli.models_list

## amora models import

::: amora.cli.models_import