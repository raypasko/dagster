import csv

from dagster import solid


@solid
def read_csv(context, csv_path):
    with open(csv_path, 'r') as fd:
        lines = [
            row for row in csv.DictReader(
                fd,
                delimiter=',',
                doublequote=False,
                escapechar='\\',
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                skipinitialspace=False,
                strict=False,
            )
        ]
    
    context.log.info(
        'Read {n_lines} lines'.format(n_lines=len(lines))
    )

    return lines
