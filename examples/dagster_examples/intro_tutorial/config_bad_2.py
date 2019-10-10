import csv

from dagster import solid


@solid
def read_csv(
    context,
    csv_path,
    delimiter,
    doublequote,
    escapechar,
    quotechar,
    quoting,
    skipinitialspace,
    strict
):
    with open(csv_path, 'r') as fd:
        lines = [
            row for row in csv.DictReader(
                fd,
                delimiter=delimiter,
                doublequote=doublequote,
                escapechar=escapechar,
                quotechar=quotechar,
                quoting=quoting,
                skipinitialspace=skipinitialspace,
                strict=strict,
            )
        ]
    
    context.log.info(
        'Read {n_lines} lines'.format(n_lines=len(lines))
    )

    return lines
