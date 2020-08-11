# Mozenda API Client

Simple API wrapper for integration with Mozenda's REST API (https://help.mozenda.com/docs/rest-api-introduction).  This API can be used to start and stop
agents (and their jobs), retrieve results, etc.

Sample code would be fully functional with valid Mozenda API Keys and operational redis cluster.  So mainly this is just example code to show what a fully Mozenda API integrated client module would like.

Here is how it would typically be used:

```
from mozenda import get_view_items, MozendaAPIError, get_xml_val, queue_task

# Retrieve results from a single running agent's view
def get_results(view_id):
  page_number = 1
  page_count = 1
  while page_number <= page_count:
    try:
      tree = get_view_items(view_id, page_number=page_number)
    except MozendaAPIError as ex:
      # Do something in response to the API's error state
      raise
    page_count = int(get_xml_val('PageCount', tree))
    for item in tree.find("ItemList"):
      yield item

# Process multiple views in a batch, careful, this will potentially run faster than Mozenda's rate limit
def process_bunch_of_views(view_list):
  for view_id in view_list:
    results = get_results(view_id)
    do_something_with_results(results)

# Use queueing to enforce Mozenda rate limits on batch operations
queue_task(view_list, process_bunch_of_views)

```

