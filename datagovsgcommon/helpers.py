'''helper functions for all datagovsg CKAN extensions'''

import calendar
import datetime
import json
import logging
import mimetypes
import re

from pylons import config
import pytz
import requests
from webhelpers.html import url_escape

import ckan.lib.datapreview as datapreview
import ckan.lib.formatters as formatters
import ckan.lib.helpers as h
import ckan.model as model
import ckan.plugins.toolkit as toolkit


log = logging.getLogger(__name__)

request_session = requests.Session()

def is_valid_url(url):
    '''
    Returns True if URL returns 200 status code
    '''
    return requests.head(url).status_code == 200

def get_config(name):
    return config.get(name)

def get_google_analytics_tracking_id():
    return config.get("ckan.datagovsg.google_analytics_tracking_id")

# This returns the groups that are available in ascending order of their
# names to the site in the homepage in /home/index.html


def get_groups():
    groups = toolkit.get_action('group_list')(
        data_dict={'sort': 'title', 'all_fields': True})
    return groups

# This returns the groups that are available in ascending order of their
# names to the site in the homepage in /home/index.html


def get_organizations():
    organizations = toolkit.get_action('organization_list')(
        data_dict={'sort': 'title', 'all_fields': True})
    return organizations


def get_packages_for_group(group, rows=10):
    query = toolkit.get_action('package_search')(data_dict={
        'q': 'groups:"%s"' % group,
        'rows': rows,
        'sort': 'metadata_modified desc'
    })
    return query['results']


def get_package_activity_list_html(id, limit=0):
    return toolkit.get_action('package_activity_list_html')(
        data_dict={'id': id, 'limit': limit})


# This returns the related media of the dataset
def get_related_list(id):
    return toolkit.get_action('related_list')(data_dict={'id': id})


def solr_escape(value):
    '''escape solr special characters'''
    ESCAPE_CHARS_RE = re.compile(r'(?<!\\)(?P<char>[&|+\-!(){}[\]^"~*?:])')
    return ESCAPE_CHARS_RE.sub(r'\\\g<char>', value)


def get_similar_datasets(package, limit=5):
    '''This returns the similar datasets for the dataset'''
    if (len(package["tags"]) == 0 and not package.get('groups')):
        return None

    query = ''
    # form the query using the tags
    for tag in package["tags"]:
        if len(query) > 0:
            query += ' OR '
        query += '"' + solr_escape(tag['name']) + '"'

    for group in package["groups"]:
        if len(query) > 0:
            query += ' OR '
        query += '"' + solr_escape(group['name']) + '"'

    query = '(' + query + ')'

    # exclude the current dataset
    query += ' -id:' + solr_escape(package['id'])

    result = toolkit.get_action('package_search')(
        data_dict={'q': query, 'rows': limit})

    results = result['results']

    return {'similar_datasets': results, 'query': query}


def get_resource_view_list(resource):
    '''This returns the inline preview of the resource'''
    return toolkit.get_action('resource_view_list')(
        data_dict={'id': resource['id']})


def get_googlemaps_client_id():
    '''returns the googlemaps client id from the config file'''
    return config.get('ckan.googlemaps.client_id', '')


def get_onemap_token():
    '''returns onemap token'''
    token = None
    access_key = config.get('ckan.datagovsg.onemap_accesskey', '')

    # retrieve token
    url = 'http://www.onemap.sg/API/services.svc/getToken'
    try:
        response = request_session.get(url, params={'accessKEY': access_key})
        if response.status_code == 200:
            token = response.json()['GetToken'][0].get('NewToken', '')
        raise requests.exceptions.RequestException
    except requests.exceptions.RequestException:
        log.error("Cannot connect to OneMap API to retrieve token.")

    return token


def get_group_image_display_url(group_name):
    '''get the group image url'''
    try:
        group = toolkit.get_action('group_show')(data_dict={'id': group_name})
        return group.get('image_display_url')
    except toolkit.ObjectNotFound:
        pass


def get_group_dropdown(pkg_dict):
    '''get the group dropdown list'''
    groups = h.groups_available()

    pkg_group_ids = set(group['id'] for group
                        in pkg_dict.get('groups', []))

    # get the group dropdown list which contain only groups that the dataset
    # does not already belongs to
    group_dropdown = [
        [
            group['id'],
            group['display_name']
        ]
        for group in groups if
        group['id'] not in pkg_group_ids
    ]

    return group_dropdown


def get_dashboard_resource(package_name=None):
    '''get the dashboard resource by package name
    returns the data'''
    if package_name:
        try:
            # get the dataset
            pkg_dict = toolkit.get_action('package_show')(
                data_dict={'id': package_name})

            # retrieve the dataset's resources
            resources = pkg_dict["resources"]
            if len(resources) > 0:
                resource = resources[0]

                resource['package_name'] = pkg_dict['name']
                resource_metadata = toolkit.get_action(
                    'resource_metadata_show')(data_dict={'id': resource['id']})
                fields = resource_metadata.get('schema', None)
                resource['fields'] = fields

                return {"title": pkg_dict['title'],
                        "resource": resource
                        }

        except (toolkit.NotAuthorized, toolkit.ObjectNotFound):
            return None


def get_datetime(hours_ago=0, date_str=None):
    '''get date with hour delta'''
    if date_str:
        local = pytz.timezone("Asia/Singapore")
        naive = h.date_str_to_datetime(date_str)
        local_dt = local.localize(naive, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)
    else:
        utc_dt = datetime.datetime.utcnow()

    utc_dt = utc_dt - datetime.timedelta(hours=hours_ago)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S")


def render_datetime(
        datetime_,
        date_format=None,
        with_hours=False,
        convert_tz=True,
        timezone="Asia/Singapore"):
    '''render date in local timezone'''
    datetime_ = h._datestamp_to_datetime(datetime_)

    if not datetime_:
        return ''

    if convert_tz:
        local = pytz.timezone(timezone)
        datetime_ = pytz.utc.localize(datetime_)
        datetime_ = datetime_.astimezone(local)

    # if date_format was supplied we use it
    if date_format:
        return datetime_.strftime(date_format)
    # the localised date
    return formatters.localised_nice_date(datetime_, show_date=True,
                                          with_hours=with_hours)


def convert_date_str_format(date_str, from_format=None, to_format=None):
    '''convert date string from one format to another'''
    if from_format:
        date = datetime.datetime.strptime(date_str, from_format)
    else:
        date = h.date_str_to_datetime(date_str)
    # if date_format was supplied we use it
    if to_format:
        return date.strftime(to_format)
    # the localised date
    return formatters.localised_nice_date(date, show_date=True)


def escape_url(value, safe='/'):
    if value:
        value = url_escape(value.encode('utf-8'), safe)
    return value


def convert_to_json_if_string(str):
    converter = toolkit.get_converter('convert_to_json_if_string')
    return converter(str, None)


def get_months():
    return calendar.month_name[1:]


def get_available_frequencies():
    frequencies = [
        {'value': 'annual', 'text': toolkit._('Annual')},
        {'value': 'half_year', 'text': toolkit._('Half-yearly')},
        {'value': 'quarterly', 'text': toolkit._('Quarterly')},
        {'value': 'monthly', 'text': toolkit._('Monthly')},
        {'value': 'weekly', 'text': toolkit._('Weekly')},
        {'value': 'daily', 'text': toolkit._('Daily')},
        {'value': 'realtime', 'text': toolkit._('Real-time')},
        {'value': 'adhoc', 'text': toolkit._('Ad-hoc')},
        {'value': 'other', 'text': toolkit._('Other')}
    ]

    return frequencies


def get_frequency(frequency):
    frequencies = filter(lambda f: frequency == f[
                         'value'], get_available_frequencies())
    if len(frequencies) > 0:
        return frequencies[0]


def get_max_resource_size():
    return config.get('ckan.max_resource_size')


def get_allowed_resource_formats():
    allowed_formats = config.get('ckan.datagovsg.allowed_resource_formats')
    allowed_formats = allowed_formats.split(' ') if allowed_formats else []
    return allowed_formats


def prettify_json(json):
    if isinstance(json, dict):
        for key, _ in json.items():
            prettified_name = key.replace('_', ' ').title()
            json[prettified_name] = prettify_json(json.pop(key))
    elif isinstance(json, list):
        return [prettify_json(obj) for obj in json]
    elif isinstance(json, basestring):
        # remove leading and trailing white spaces, new lines, tabs
        json = json.strip(' \t\n\r')
    return json


def prettify_string(string=''):
    string = re.sub('(?<!\w)[Uu]rl(?!\w)', 'URL',
                    string.replace('_', ' ').capitalize())
    return string.replace('_', ' ')


def get_field_descriptions(field):
    descriptions = []
    if field.get('description'):
        for desc in field.get('description').splitlines():
            descriptions.append(desc)

    # add the financial year start and end date into description
    if 'financial' in field.get('sub_type'):
        financial = field.get('financial')
        start = '%s %s' % (financial['start_day'], calendar.month_name[
                           int(financial['start_month'])])
        end = '%s %s' % (financial['end_day'], calendar.month_name[
                         int(financial['end_month'])])
        financial_desc = 'Financial year starts on %s and ends on %s' % (
            start, end)
        descriptions.append(toolkit._(financial_desc))

    # add the percentage type footnote into description
    if field['type'] == 'numeric' and field['sub_type'] == 'percentage':
        descriptions.append(
            toolkit._(
                'Percentages are expressed as a value over %s, i.e. "%s" represents 100%%' %
                (field['percentage_type'], field['percentage_type'])))

    # add the null value footnotes into description
    if field.get('null_values'):
        null_values = field.get('null_values')
        if 'na' in null_values:
            desc = '"na" : '
            if null_values['na']:
                desc += null_values['na']
            else:
                desc += 'Data not available or not applicable'
            descriptions.append(toolkit._(desc))
        if '-' in null_values:
            desc = '"-" : '
            if null_values['-']:
                desc += null_values['-']
            else:
                desc += 'Data is negligible or not significant'
            descriptions.append(toolkit._(desc))
        if 's' in null_values:
            desc = '"s" : '
            if null_values['s']:
                desc += null_values['s']
            else:
                desc += 'Data is suppressed'
            descriptions.append(toolkit._(desc))

    return descriptions

# get the export options for the view


def resource_view_export_options(resource_view):
    view_plugin = datapreview.get_view_plugin(resource_view['view_type'])
    return view_plugin.info().get('export_options', None)

# get the default for the view


def resource_view_default_title(view_type):
    view_plugin = datapreview.get_view_plugin(view_type)
    return view_plugin.info().get('default_title', '')


def get_card_types(value=None):
    card_types = [{'value': 'line', 'text': 'Line Graph'},
                  {'value': 'column', 'text': 'Column Graph'},
                  {'value': 'stacked', 'text': 'Stacked Bar Graph'},
                  {'value': 'bar', 'text': 'Horizontal Bar Graph'},
                  {'value': 'pie', 'text': 'Pie Chart'},
                  {'value': 'table', 'text': 'Table'},
                  {'value': 'numbers', 'text': 'Numbers'},
                  {'value': 'map', 'text': 'Map'}]
    if value:
        return next(
            (type for type in card_types if value == type['value']),
            None)
    else:
        return card_types

# returns the list of dashboards for each page


def dashboard_list(page_id=None):
    return toolkit.get_action('dashboard_show')(data_dict={'page': page_id})

# returns the dashboard card


def get_dashboard_card(card_id, size=4):
    try:
        card_dict = toolkit.get_action(
            'dashboard_card_show')(data_dict={'id': card_id})
        if card_dict:
            card_dict['size'] = size
            return card_dict
    # exclude the cards where the dataset or resource can't be found
    except toolkit.ObjectNotFound:
        pass


def can_delete_card(card):
    return card.get('id', None) and len(card.get('pages', [])) == 0


def get_developers_site_url():
    return config.get('ckan.datagovsg.developers_site_url')


def show_sysadmin_fields(package_id):
    context = {'model': model, 'session': model.Session,
               'user': toolkit.c.user}
    package = toolkit.get_action('package_show')(context, {'id': package_id})
    if package.get('sysadmin_edit_only', False):
        if h.check_access('sysadmin'):
            return True

    return False


def get_available_realtime_frequencies():
    frequencies = [{'value': str(5 * 1000), 'text': toolkit._('Every 5 seconds')},
                   {'value': str(10 * 1000), 'text': toolkit._('Every 10 seconds')},
                   {'value': str(20 * 1000), 'text': toolkit._('Every 20 seconds')},
                   {'value': str(30 * 1000), 'text': toolkit._('Every 30 seconds')}, 
                   {'value': str(60 * 1000), 'text': toolkit._('Every 1 minute')},
                   {'value': str(60 * 5 * 1000), 'text': toolkit._('Every 5 minutes')},
                   {'value': str(60 * 30 * 1000), 'text': toolkit._('Every 30 minutes')},
                   {'value': str(60 * 60 * 1000), 'text': toolkit._('Every 1 hour')}]

    return frequencies


def get_realtime_frequency(frequency):
    frequencies = filter(lambda f: frequency == f[
                         'value'], get_available_realtime_frequencies())
    if len(frequencies) > 0:
        return frequencies[0]


def is_downloadable_url(url):
    '''check whether the url is a downloadable file'''
    content_type, _ = mimetypes.guess_type(url)
    if content_type and content_type != 'text/html':
        return True
    return False


def get_gis_resource_formats():
    gis_formats = config.get('ckan.datagovsg.gis_resource_formats')
    gis_formats = gis_formats.split(' ') if gis_formats else []
    return gis_formats


def get_task_status(entity_id, task_type, key=None):
    if not key:
        key = task_type
    data_dict = {
        'entity_id': entity_id,
        'task_type': task_type,
        'key': key
    }
    task = {}
    try:
        task = toolkit.get_action('task_status_show')(data_dict=data_dict)
        task['error'] = json.loads(task['error'])
        if task['error']:
            task['error_summary'] = prettify_json(dict(task['error']))
        task['value'] = json.loads(task['value'])
        if task.get('state'):
            task['status_description'] = task['state'].capitalize()
    except toolkit.ObjectNotFound:
        pass

    return task


def get_last_updated_date():
    '''get the last updated date based on the site revisions and deployment date'''
    last_updated_date = None
    # get the modified date of last updated object
    revisions = toolkit.get_action('revision_list')({'ignore_auth': True})
    if len(revisions) > 0:
        last_revision = toolkit.get_action('revision_show')(
            {'ignore_auth': True}, {'id': revisions[0]})
        last_updated_date = h.date_str_to_datetime(last_revision['timestamp'])

    # get last deployment date from config
    if config.get('ckan.datagovsg.last_updated'):
        last_updated_date = max([last_updated_date, h.date_str_to_datetime(
            config.get('ckan.datagovsg.last_updated'))])

    if not last_updated_date:
        last_updated_date = datetime.datetime.utcnow()

    return render_datetime(last_updated_date)
