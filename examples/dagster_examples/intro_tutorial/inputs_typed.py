import csv

from dagster import execute_pipeline, pipeline, solid


@solid
def read_csv(context, csv_path: str):
    with open(csv_path, 'r') as fd:
        lines = [row for row in csv.DictReader(fd)]
    
    context.log.info(
        'Read {n_lines} lines'.format(n_lines=len(lines))
    )
    return lines


@solid
def sort_by_calories(context, cereals):
    sorted_cereals = sorted(cereals, key=lambda cereal: cereal['calories'])
    context.log.info(
        'Least caloric cereal: {least_caloric}'.format(
            least_caloric=sorted_cereals[0]['name']
        )
    )
    context.log.info(
        'Most caloric cereal: {most_caloric}'.format(
            most_caloric=sorted_cereals[-1]['name']
        )
    )


@pipeline
def inputs_pipeline():
    sort_by_calories(read_csv())


if __name__ == '__main__':
    environment_dict = {
        'solids': {
            'read_csv': {'inputs': {'csv_path': {'value': 'cereal.csv'}}}
        }
    }
    result = execute_pipeline(
        inputs_pipeline, environment_dict=environment_dict
    )
    assert result.success

# def execute_with_another_world():
#     return execute_pipeline(
#         hello_inputs_pipeline,
#         # This entire dictionary is known as the 'environment'.
#         # It has many sections.
#         {
#             # This is the 'solids' section
#             'solids': {
#                 # Configuration for the add_hello_to_word solid
#                 'add_hello_to_word': {'inputs': {'word': {'value': 'Mars'}}}
#             }
#         },
#     )
