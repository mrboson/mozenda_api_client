from __future__ import absolute_import, division, unicode_literals, print_function

from logging import getLogger
import time

from celery import shared_task
import copy
import settings
import requests
from six.moves.urllib.parse import urlencode
from xml.etree import ElementTree

logger = getLogger()

API_ROOT = getattr(settings, 'MOZENDA_BASE_URL', 'https://api.mozenda.com')
TEST_MODE = settings.ENVIRONMENT == 'dev'

# Mozenda allows 60 queries per minute
# So we do 60 queries every 65 seconds to account for any discrepancies
MOZENDA_CONFIG = {
    'api_key': settings.MOZENDA_API_KEY,
    'queries': getattr(settings, 'MOZENDA_QUERIES_PER_TIMEFRAME', 60),
    'timeframe': getattr(settings, 'MOZENDA_QUERY_TIMEFRAME', 65),
    'max_pages': getattr(settings, 'MOZENDA_MAX_PAGES', 2),
}

# Priority fetchers run with a different API key
# and have higher rate limits
MOZENDA_PRIORITY_CONFIG = copy.copy(MOZENDA_CONFIG)
MOZENDA_PRIORITY_CONFIG['api_key'] = settings.MOZENDA_PRIORITY_API_KEY
MOZENDA_PRIORITY_CONFIG['queries'] = 60
MOZENDA_FETCH_QUEUE = 'mozenda'
MOZENDA_MANAGEMENT_QUEUE = 'mozenda_mgmt'

R_DEFAULT = settings.REDIS['default']
QUEUE_COUNTER_KEY = 'MozendaQueueCounter:{}'


def fetch_queue_size():
    size = R_DEFAULT.get(QUEUE_COUNTER_KEY.format(MOZENDA_FETCH_QUEUE))
    if size:
        return int(size)
    else:
        return 0


def management_queue_size():
    size = R_DEFAULT.get(QUEUE_COUNTER_KEY.format(MOZENDA_MANAGEMENT_QUEUE))
    if size:
        return int(size)
    else:
        return 0


def api_endpoint(service='Mozenda10', api_name='rest', **kwargs):
    bare_keys = []
    valued_args = {
        'WebServiceKey': kwargs.pop('api_key', None) or MOZENDA_CONFIG['api_key'],
        'service': service
    }
    for key, value in kwargs.items():
        # Mozenda supports parameters with dot (.) syntax.  We encode that with __
        key = key.replace('__', '.')

        if value is None:
            bare_keys.append(key)
        else:
            valued_args[key] = value

    if valued_args or bare_keys:
        api_name += (
            '?' +
            (valued_args and urlencode(valued_args) or '') +
            ((valued_args and bare_keys) and '&' or '') +
            (bare_keys and '&'.join(bare_keys) or '')
        )
    return '{0}/{1}'.format(API_ROOT, api_name)


def get_collection_list(api_key=None):
    response = _api_call(Operation='Collection.GetList', ResponseFormat='JSON', api_key=api_key)
    if response:
        for item in response.get("Collection"):
            yield item
    else:
        return None


def get_agent_list(api_key=None):
    response = _api_call(Operation='Agent.GetList', ResponseFormat='JSON', api_key=api_key)
    if response:
        for item in response.get("Agent"):
            yield item
    else:
        return None


def get_agent(agent_id, api_key=None):
    if agent_id:
        return _api_call(Operation='Agent.Get', AgentId=agent_id, ResponseFormat='JSON', api_key=api_key)
    else:
        return None


def get_agent_jobs(agent_id, state='All', created_after=None, api_key=None):
    api_args = {
        'Operation': 'Agent.GetJobs',
        'AgentId': agent_id,
        'Job__State': state,
        'ResponseFormat': 'JSON'
    }
    if created_after:
        api_args['Job__Created'] = created_after.strftime('%Y-%m-%d')
    if api_key:
        api_args['api_key'] = api_key

    response = _api_call(**api_args)
    if response:
        for item in response.get("Job"):
            yield item
    else:
        return None


def get_collection_views(collection_id, api_key=None):
    response = _api_call(
        Operation='Collection.GetViews', CollectionID=collection_id, ResponseFormat='JSON', api_key=api_key
    )
    if response:
        for item in response.get("View"):
            yield item
    else:
        return None


def get_jobs(job_ids, api_key=None):
    # JobID expects a comma-separated list
    if isinstance(job_ids, list):
        # The list can either be strings or integers
        job_ids = ','.join([str(i) for i in job_ids])
    response = _api_call(Operation='Job.Get', JobID=job_ids, ResponseFormat='JSON', api_key=api_key)
    jobs = response.get("Job", [])
    if isinstance(jobs, dict):
        jobs = [jobs]
    for item in jobs:
        yield item


def get_job_list(state='Active', created_after=None, api_key=None):
    api_args = {
        'Operation': 'Job.GetList',
        'Job__State': state,
        'ResponseFormat': 'JSON'
    }
    if created_after:
        api_args['Job__Created'] = created_after.strftime('%Y-%m-%d')
    if api_key:
        api_args['api_key'] = api_key

    response = _api_call(**api_args)
    if response:
        for item in response.get("Job"):
            yield item
    else:
        return None


def job_action(job_id, action, api_key=None, wait_for_result=False):
    operation = 'Job.{}'.format(action.title())
    if TEST_MODE:
        logger.debug('Mozenda API: [{}] DEV - skipping {} {}'.format(operation, action, job_id))
    else:
        response = _api_call(Operation=operation, JobID=job_id, ResponseFormat='JSON', api_key=api_key)
        if response.get('JsonResult', {}).get('Result') == 'Success':
            if wait_for_result:
                # Call the Job.<Action> API to get our job's agent ID, then poll agent status
                # until we get the "Ready" state
                try:
                    job = list(get_jobs(job_id, api_key=api_key))[0]
                except Exception as e:
                    error_message = 'Unable to get status for Job Action'
                    raise MozendaAPIResponseError(
                        'Mozenda Response: Error - {} - Job ID {}'.format(error_message, job_id),
                        error_code='JobActionStatus',
                        error_message=str(e)
                    )
                agent_id = job['ScheduleItemID']
                status = ''
                # Make sure our while loop doesn't run forever... Give Mozenda 5 minutes
                start_time = time.time()
                while status != 'Ready' and time.time() - start_time < 300:
                    time.sleep(5)
                    agent = get_agent(agent_id, api_key=api_key)
                    status = agent.get('Status')
                if status != 'Ready':
                    error_message = 'Timeout waiting for Job Action to complete'
                    raise MozendaAPIResponseError(
                        'Mozenda Response: Error - {} - Job ID {}'.format(error_message, job_id),
                        error_code='JobActionTimeout',
                        error_message=error_message
                    )
            logger.info('Mozenda API: [{}] {} Job ID {}'.format(operation, action, job_id))
        else:
            return False
    return True


def delete_agent(agent_id, api_key=None):
    if TEST_MODE:
        logger.debug('Mozenda API: [Agent.Delete] DEV - skipping delete {}'.format(agent_id))
    else:
        response = _api_call(Operation='Agent.Delete', AgentId=agent_id, ResponseFormat='JSON', api_key=api_key)
        if response.get('JsonResult', {}).get('Result') == 'Success':
            logger.info('Mozenda API: [Agent.Delete] deleted Agent ID {}'.format(agent_id))
    return True


def move_agent(agent_id, api_key=None, destination_account=None, include_schedule=False):
    """ We will call the API Agent.Copy function, and if that is successful, we will get a Batch ID
    which we can use to poll the API for the copy task's completion.  If the copy task completes, we will
    then delete the original agent.  We do a Copy-Delete because we don't trust Mozenda's Move function to
    be transactional.

    This will take some time... so this function usually needs to be invoked from a Celery task """
    batch_id = None
    if TEST_MODE:
        logger.debug('Mozenda API: [Agent.Move] DEV - skipping move {} to {}'.format(agent_id, destination_account))
    else:
        try:
            response = _api_call(
                api_key=api_key,
                Operation='Agent.Copy',
                AgentID=agent_id,
                DestinationAccountKey=destination_account,
                CopyData=True,
                IncludeAgentRunSchedule=include_schedule,
                IncludeAgentNotifications=True,
                ResponseFormat='JSON'
            )
        except MozendaAPIResponseError as e:
            if e.error_code == 'AgentMoveFailed' and e.error_message == 'This agent currently has active jobs.':
                raise MozendaAgentHasRunningJobs(
                    'Agent move failed - This agent currently has active jobs',
                    error_code='AgentRunningJobs'
                )

            logger.warning('Mozenda API: [Agent.Move] error moving {} to {} - {}'.format(
                agent_id,
                destination_account,
                str(e)
            ))
            return batch_id
        if 'BatchID' not in response:
            logger.warning('Mozenda API: [Agent.Move] error moving {} to {}'.format(agent_id, destination_account))
        else:
            batch_id = response.get("BatchID")

    return batch_id


def agent_transfer_status(batch_id, api_key=None):
    response = _api_call(
        api_key=api_key,
        Operation='Agent.GetMoveCopyStatus',
        ResponseFormat='JSON',
        BatchID=batch_id
    )

    if response.get('ErrorCode'):
        error_message = response.get('ErrorMessage', 'Unknown Error')
        raise MozendaAPIResponseError(
            'Agent move failed - Agent.GetMoveCopyStatus - {}'.format(error_message),
            error_code=response.get('ErrorCode'),
            error_message=error_message
        )

    # Progress will be empty when the job is done
    running_state = response.get('Progress')
    status = running_state or 'DONE'

    logger.info('Mozenda API: [Agent.GetMoveCopyStatus] Batch ID {} - Progress {}'.format(batch_id, status))

    return not running_state


def get_view_items(view_id, api_key=None, item_count=1000, page_number=1):
    return _api_call(
        api_key=api_key,
        Operation='View.GetItems',
        ViewID=view_id,
        PageItemCount=item_count,
        PageNumber=page_number
    )


def get_xml_val(key, item, default=None):
    try:
        return item.find(key).text
    except AttributeError:
        return default


@shared_task
def queued_task_done(*args, **kwargs):
    queue = kwargs.pop('queue')
    error = kwargs.pop('error', False)
    if error:
        # args[0] - task request
        # args[1] - exc
        # args[2] - traceback
        logger.error('Mozenda API: Error completing task - {} - {}'.format(queue, args))
    if queue:
        # Tick off one task and reset the counter key expiration
        R_DEFAULT.decr(QUEUE_COUNTER_KEY.format(queue))
        R_DEFAULT.expire(QUEUE_COUNTER_KEY.format(queue), 1200)


def queue_task(item_list, task, *args, **kwargs):
    """ Mozenda API calls are rate limited, so we handle all queuing of Mozenda tasks here so we can
        control task timing. """

    queue = kwargs.pop('queue', MOZENDA_MANAGEMENT_QUEUE)

    if queue == MOZENDA_MANAGEMENT_QUEUE:
        # Management queue tasks need to be sensitive to what has already been queued so that
        # fetcher tasks keep the priority.  So we will start with an existing queue count
        initial_count = management_queue_size() + fetch_queue_size()
    else:
        initial_count = fetch_queue_size()

    # We also keep our own number of queued items in a redis key so we have a rough
    # idea of where we are in our processing... also, set this key to expire in 20 minutes
    R_DEFAULT.incrby(QUEUE_COUNTER_KEY.format(queue), len(item_list))
    R_DEFAULT.expire(QUEUE_COUNTER_KEY.format(queue), 1200)

    for counter, item in enumerate(item_list):
        # We need to implement a task countdown in order to keep under the Mozenda API rate limit
        batch = (initial_count + counter) / MOZENDA_CONFIG['queries']
        countdown = batch * MOZENDA_CONFIG['timeframe']

        try:
            task_args = [item]
            task_args += args
            task.apply_async(
                args=task_args,
                kwargs=kwargs,
                queue=queue,
                countdown=countdown,
                link=queued_task_done.s(queue=queue),
                link_error=queued_task_done.s(queue=queue, error=True)
            )
        except:
            logger.exception("Mozenda Queue: Error queuing item {}.".format(item))


def _api_call(**kwargs):
    try:
        response = requests.get(api_endpoint(**kwargs))
        response.raise_for_status()
    except Exception as ex:
        error_msg = 'Mozenda API: Error - Failed perform API call on operation {}. ' \
                    'Error: {}'.format(kwargs.pop('Operation', None), ex)
        logger.debug(error_msg)
        raise MozendaAPIError(error_msg)

    response_error = {}
    if 'ResponseFormat' in kwargs and kwargs['ResponseFormat'] == 'JSON':
        try:
            result = response.json()
        except Exception as ex:
            error_msg = 'Mozenda API: Error - Unable to load JSON. ' \
                        'Error: {}'.format(kwargs.pop('Operation', None), ex)
            raise MozendaAPIParseError(error_msg)

        if result is None:
            raise MozendaAPIParseError('Mozenda API: Error - Unable to load JSON. Empty response')

        if 'JsonResult' not in result or result['JsonResult'].get('Result', '') != 'Success':
            response_error['error_code'] = result['JsonResult'].get('ErrorCode')
            response_error['error_message'] = result['JsonResult'].get('ErrorDescription', 'Unknown Mozenda Error')
    else:
        try:
            result = ElementTree.fromstring(response.content)
        except Exception as ex:
            error_msg = 'Mozenda API: Error - Unable to load XML. ' \
                        'Error: {}'.format(kwargs.pop('Operation', None), ex)
            raise MozendaAPIParseError(error_msg)

        if result is None or len(result) == 0:
            raise MozendaAPIParseError('Mozenda API: Error - Unable to load XML. XML doc is empty')

        if get_xml_val('Result', result) != 'Success':
            response_error['error_code'] = get_xml_val('ErrorCode', result)
            response_error['error_message'] = get_xml_val('ErrorMessage', result, default='Unknown Mozenda Error')

    if response_error:
        error_message = response_error.get('error_message')
        if response_error.get('error_code', '') in ('AgentNotFound', 'MissingJobID'):
            raise MozendaObjectNotFound(
                'Mozenda Response: Error - {}'.format(error_message),
                error_code=response_error.get('error_code')
            )
        else:
            raise MozendaAPIResponseError(
                'Mozenda Response: Error - {}'.format(error_message),
                error_code=response_error.get('error_code'),
                error_message=response_error.get('error_message')
            )

    return result


class MozendaAPIError(Exception):
    pass


class MozendaAPIParseError(MozendaAPIError):
    pass


class MozendaAPIResponseError(MozendaAPIError):
    def __init__(self, msg, error_code=None, error_message=None):
        self.error_code = error_code
        self.error_message = error_message
        super().__init__(msg)


class MozendaObjectNotFound(MozendaAPIResponseError):
    pass


class MozendaAgentHasRunningJobs(MozendaAPIResponseError):
    pass
