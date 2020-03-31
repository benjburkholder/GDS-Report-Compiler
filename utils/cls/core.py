"""
Custom

This script is where all reporting configuration takes place
"""
import re


class Customizer:
    """
    Required to run scripts
    Manages all report data transformation and customization
    """
    # GLOBALS - REQUIRED TO BE REFERENCED FOR ALL PROJECTS
    required_attributes = [
        'dbms',
        'client',
        'project',
        'version',
        'recipients'
    ]

    def build_url_backfilter_statement(self, target_table=None):
        stmt = f"UPDATE public.{target_table} TARGET\n"
        stmt += "SET property = LOOKUP.property\n"
        stmt += f"FROM public.lookup_urltolocation LOOKUP\n"
        stmt += "WHERE TARGET.url ILIKE CONCAT('%', LOOKUP.url, '%');\n"

        stmt2 = f"UPDATE public.{target_table} TARGET\n"
        stmt2 += "SET property = LOOKUP.property\n"
        stmt2 += f"FROM public.lookup_urltolocation LOOKUP\n"
        stmt2 += "WHERE TARGET.url ILIKE CONCAT('%', LOOKUP.url, '%')\n"
        stmt2 += "AND LOOKUP.exact = 1;"

        stmt3 = f"UPDATE public.{target_table}\n"
        stmt3 += "SET property = 'Non-Location Pages'\n"
        stmt3 += "WHERE property IS NULL;\n"

        return [stmt, stmt2, stmt3]

    def build_moz_backfilter_statement(self, target_table=None):
        stmt = f"UPDATE public.{target_table} TARGET\n"
        stmt += "SET property = LOOKUP.property\n"
        stmt += f"FROM public.lookup_moz_listingtolocation LOOKUP\n"
        stmt += "WHERE CAST(TARGET.listing_id AS character varying(25)) = CAST(LOOKUP.listing_id AS character varying(25));\n"

        stmt2 = f"UPDATE public.{target_table}\n"
        stmt2 += "SET property = 'Non-Location Pages'\n"
        stmt2 += "WHERE property IS NULL;\n"

        return [stmt, stmt2]

    CLIENT_NAME = 'ZwirnerEquipment'

    CONFIGURATION_WORKBOOK = {
        'config_sheet_name': 'Zwirner Equipment - Configuration',
        'source_refresh_dates': [1, 15],
        'lookup_refresh_status': False,

        'sheets': [
            {'sheet': 'URL to Property', 'table': {
                'name': 'lookup_urltolocation',
                'schema': 'public',
                'type': 'lookup',
                'columns': [
                    {'name': 'url', 'type': 'character varying', 'length': 100},
                    {'name': 'property', 'type': 'character varying', 'length': 100},
                    {'name': 'exact', 'type': 'bigint'},
                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Moz Pro Campaign Master', 'table': {
                'name': 'source_moz_procampaignmaster',
                'schema': 'public',
                'type': 'source',
                'columns': [
                    {'name': 'campaign_id', 'type': 'character varying', 'length': 100},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Moz Local Account Master', 'table': {
                'name': 'source_moz_localaccountmaster',
                'schema': 'public',
                'type': 'source',
                'columns': [
                    {'name': 'account', 'type': 'character varying', 'length': 150},
                    {'name': 'label', 'type': 'character varying', 'length': 150},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Moz Local Account Master', 'table': {
                'name': 'source_moz_localaccountmaster',
                'schema': 'public',
                'type': 'source',
                'columns': [
                    {'name': 'account', 'type': 'character varying', 'length': 150},
                    {'name': 'label', 'type': 'character varying', 'length': 150},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'GA Views', 'table': {
                'name': 'source_ga_views',
                'schema': 'public',
                'type': 'source',
                'columns': [
                    {'name': 'view_id', 'type': 'character varying', 'length': 100},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Moz Listing to Property', 'table': {
                'name': 'lookup_moz_listingtolocation',
                'schema': 'public',
                'type': 'lookup',
                'columns': [
                    {'name': 'listing_id', 'type': 'character varying', 'length': 100},
                    {'name': 'property', 'type': 'character varying', 'length': 150},
                    {'name': 'account', 'type': 'character varying', 'length': 150},
                    {'name': 'label', 'type': 'character varying', 'length': 150},
                    {'name': 'name', 'type': 'character varying', 'length': 150},
                    {'name': 'address', 'type': 'character varying', 'length': 250},
                    {'name': 'city', 'type': 'character varying', 'length': 50},
                    {'name': 'state', 'type': 'character varying', 'length': 50},
                    {'name': 'zip', 'type': 'bigint'},
                    {'name': 'phone', 'type': 'character varying', 'length': 25},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'GMB Listing to Property', 'table': {
                'name': 'lookup_gmb_listingtolocation',
                'schema': 'public',
                'type': 'lookup',
                'columns': [
                    {'name': 'listing_id', 'type': 'character varying', 'length': 100},
                    {'name': 'property', 'type': 'character varying', 'length': 150},
                    {'name': 'address_line_1', 'type': 'character varying', 'length': 250},
                    {'name': 'city', 'type': 'character varying', 'length': 50},
                    {'name': 'state', 'type': 'character varying', 'length': 50},
                    {'name': 'zip', 'type': 'character varying', 'length': 50},
                    {'name': 'phone', 'type': 'character varying', 'length': 25},

                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'moz_local_visibility_report_mdd',
                'schema': 'public',
                'type': 'reporting',
                'backfilter': build_moz_backfilter_statement,
                'columns': [
                    {'name': 'report_date', 'type': 'date'},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100},
                    {'name': 'property', 'type': 'character varying', 'length': 100},
                    {'name': 'account_name', 'type': 'character varying', 'length': 100},
                    {'name': 'listing_id', 'type': 'character varying', 'length': 25},
                    {'name': 'directory', 'type': 'character varying', 'length': 100},
                    {'name': 'points_reached', 'type': 'bigint'},
                    {'name': 'max_points', 'type': 'bigint'},

                ],
                'indexes': [
                    {
                        'name': 'ix_moz_local_directory_visibility_report',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'data_source', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'property', 'sort': 'asc', 'nulls_last': True}
                        ]
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'moz_local_sync_report_mdd',
                'schema': 'public',
                'type': 'reporting',
                'backfilter': build_moz_backfilter_statement,
                'columns': [
                    {'name': 'report_date', 'type': 'date'},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100},
                    {'name': 'property', 'type': 'character varying', 'length': 100},
                    {'name': 'account_name', 'type': 'character varying', 'length': 100},
                    {'name': 'listing_id', 'type': 'character varying', 'length': 25},
                    {'name': 'directory', 'type': 'character varying', 'length': 100},
                    {'name': 'field', 'type': 'character varying', 'length': 100},
                    {'name': 'sync_status', 'type': 'bigint'},

                ],
                'indexes': [
                    {
                        'name': 'ix_moz_local_directory_sync_report',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'data_source', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'property', 'sort': 'asc', 'nulls_last': True}
                        ]
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'moz_pro_rankings',
                'schema': 'public',
                'type': 'reporting',
                'backfilter': build_url_backfilter_statement,
                'columns': [
                    {'name': 'report_date', 'type': 'date'},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100},
                    {'name': 'property', 'type': 'character varying', 'length': 100},
                    {'name': 'campaign_id', 'type': 'character varying', 'length': 100},
                    {'name': 'id', 'type': 'character varying', 'length': 100},
                    {'name': 'search_id', 'type': 'character varying', 'length': 100},
                    {'name': 'keyword', 'type': 'character varying', 'length': 100},
                    {'name': 'search_engine', 'type': 'character varying', 'length': 100},
                    {'name': 'device', 'type': 'character varying', 'length': 100},
                    {'name': 'geo', 'type': 'character varying', 'length': 100},
                    {'name': 'tags', 'type': 'character varying', 'length': 250},
                    {'name': 'url', 'type': 'character varying', 'length': 1000},
                    {'name': 'keyword_added_at', 'type': 'timestamp with time zone'},
                    {'name': 'rank', 'type': 'bigint'},
                    {'name': 'branded', 'type': 'bigint'},

                ],
                'indexes': [
                    {
                        'name': 'ix_moz_pro_rankings',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'id', 'sort': 'asc', 'nulls_last': True},
                        ]
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'moz_pro_serp',
                'schema': 'public',
                'type': 'reporting',
                'backfilter': build_url_backfilter_statement,
                'columns': [
                    {'name': 'report_date', 'type': 'date'},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100},
                    {'name': 'property', 'type': 'character varying', 'length': 100},
                    {'name': 'campaign_id', 'type': 'character varying', 'length': 100},
                    {'name': 'id', 'type': 'character varying', 'length': 100},
                    {'name': 'search_id', 'type': 'character varying', 'length': 100},
                    {'name': 'keyword', 'type': 'character varying', 'length': 100},
                    {'name': 'search_engine', 'type': 'character varying', 'length': 100},
                    {'name': 'device', 'type': 'character varying', 'length': 100},
                    {'name': 'geo', 'type': 'character varying', 'length': 100},
                    {'name': 'tags', 'type': 'character varying', 'length': 250},
                    {'name': 'url', 'type': 'character varying', 'length': 1000},
                    {'name': 'keyword_added_at', 'type': 'timestamp with time zone'},
                    {'name': 'ads_bottom', 'type': 'bigint'},
                    {'name': 'ads_top', 'type': 'bigint'},
                    {'name': 'featured_snippet', 'type': 'bigint'},
                    {'name': 'image_pack', 'type': 'bigint'},
                    {'name': 'in_depth_articles', 'type': 'bigint'},
                    {'name': 'knowledge_card', 'type': 'bigint'},
                    {'name': 'knowledge_panel', 'type': 'bigint'},
                    {'name': 'local_pack', 'type': 'bigint'},
                    {'name': 'local_teaser', 'type': 'bigint'},
                    {'name': 'news_pack', 'type': 'bigint'},
                    {'name': 'related_questions', 'type': 'bigint'},
                    {'name': 'review', 'type': 'bigint'},
                    {'name': 'shopping_results', 'type': 'bigint'},
                    {'name': 'site_links', 'type': 'bigint'},
                    {'name': 'tweet', 'type': 'bigint'},
                    {'name': 'video', 'type': 'bigint'},
                    {'name': 'branded', 'type': 'bigint'},

                ],
                'indexes': [
                    {
                        'name': 'ix_moz_pro_serp',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'id', 'sort': 'asc', 'nulls_last': True},
                        ]
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'google_analytics_traffic',
                'schema': 'public',
                'type': 'reporting',
                'backfilter': build_url_backfilter_statement,
                'columns': [
                    {'name': 'report_date', 'type': 'date'},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100},
                    {'name': 'channel_grouping', 'type': 'character varying', 'length': 100},
                    {'name': 'property', 'type': 'character varying', 'length': 100},
                    {'name': 'service_line', 'type': 'character varying', 'length': 100},
                    {'name': 'view_id', 'type': 'character varying', 'length': 25},
                    {'name': 'source_medium', 'type': 'character varying', 'length': 100},
                    {'name': 'device', 'type': 'character varying', 'length': 50},
                    {'name': 'campaign', 'type': 'character varying', 'length': 100},
                    {'name': 'url', 'type': 'character varying', 'length': 500},
                    {'name': 'sessions', 'type': 'character varying', 'length': 500},
                    {'name': 'percent_new_sessions', 'type': 'double precision'},
                    {'name': 'pageviews', 'type': 'bigint'},
                    {'name': 'unique_pageviews', 'type': 'bigint'},
                    {'name': 'pageviews_per_session', 'type': 'double precision'},
                    {'name': 'entrances', 'type': 'bigint'},
                    {'name': 'bounces', 'type': 'bigint'},
                    {'name': 'session_duration', 'type': 'double precision'},
                    {'name': 'users', 'type': 'bigint'},
                    {'name': 'new_users', 'type': 'bigint'},
                ],
                'indexes': [
                    {
                        'name': 'ix_google_analytics_traffic',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'source_medium', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'device', 'sort': 'asc', 'nulls_last': True}
                        ]
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'google_analytics_events',
                'schema': 'public',
                'type': 'reporting',
                'backfilter': build_url_backfilter_statement,
                'columns': [
                    {'name': 'report_date', 'type': 'date'},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100},
                    {'name': 'channel_grouping', 'type': 'character varying', 'length': 200},
                    {'name': 'property', 'type': 'character varying', 'length': 100},
                    {'name': 'service_line', 'type': 'character varying', 'length': 100},
                    {'name': 'view_id', 'type': 'character varying', 'length': 25},
                    {'name': 'source_medium', 'type': 'character varying', 'length': 100},
                    {'name': 'device', 'type': 'character varying', 'length': 50},
                    {'name': 'campaign', 'type': 'character varying', 'length': 100},
                    {'name': 'url', 'type': 'character varying', 'length': 500},
                    {'name': 'event_label', 'type': 'character varying', 'length': 200},
                    {'name': 'event_action', 'type': 'character varying', 'length': 200},
                    {'name': 'total_events', 'type': 'bigint'},
                    {'name': 'unique_events', 'type': 'bigint'},
                    {'name': 'event_value', 'type': 'double precision'},

                ],
                'indexes': [
                    {
                        'name': 'ix_google_analytics_events',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'source_medium', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'device', 'sort': 'asc', 'nulls_last': True}
                        ]
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'google_analytics_goals',
                'schema': 'public',
                'type': 'reporting',
                'backfilter': build_url_backfilter_statement,
                'columns': [
                    {'name': 'report_date', 'type': 'date'},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100},
                    {'name': 'channel_grouping', 'type': 'character varying', 'length': 150},
                    {'name': 'property', 'type': 'character varying', 'length': 100},
                    {'name': 'service_line', 'type': 'character varying', 'length': 100},
                    {'name': 'view_id', 'type': 'character varying', 'length': 25},
                    {'name': 'source_medium', 'type': 'character varying', 'length': 100},
                    {'name': 'device', 'type': 'character varying', 'length': 50},
                    {'name': 'campaign', 'type': 'character varying', 'length': 100},
                    {'name': 'url', 'type': 'character varying', 'length': 500},
                    {'name': 'request_a_quote', 'type': 'bigint'},
                    {'name': 'sidebar_contact_us', 'type': 'bigint'},
                    {'name': 'contact_us_form_submission', 'type': 'bigint'},
                    {'name': 'newsletter_signups', 'type': 'bigint'},
                    {'name': 'dialogtech_calls', 'type': 'bigint'},

                ],
                'indexes': [
                    {
                        'name': 'ix_google_analytics_goals',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'source_medium', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'device', 'sort': 'asc', 'nulls_last': True}
                        ]
                    }
                ],
                'owner': 'postgres'
            }}

        ]}

    supported_dbms = [
        'postgresql'
    ]

    global_configuration_message = "Invalid global configuration. Please check your Customizer class and try again"

    # ### START EDITING HERE ###
    dbms = 'postgresql'
    client = 'ZwirnerEquipment'
    project = '<PROJECT>'
    version = '<VERSION>'
    recipients = [
        # EMAILS HERE for error notifications
        'jschroeder@linkmedia360.com',
        'bburkholder@linkmedia360.com'
    ]
    db = {
        'DATABASE': 'zwirnerequipment_omnilocal',
        'USERNAME': 'python-2',
        'PASSWORD': 'pythonpipelines',
        'SERVER': '35.222.11.147'
    }

    # ### END EDITING ###

    def __init__(self):
        self.prefix = self.get_class_prefix()
        self.set_function_prefixes()
        assert self.valid_global_configuration(), self.global_configuration_message

    def valid_global_configuration(self) -> bool:
        for attribute in self.required_attributes:
            if not getattr(self, attribute):
                return False
            if attribute == 'dbms':
                if getattr(self, attribute) not in self.supported_dbms:
                    return False
        return True

    def get_class_prefix(self):
        cls_name = self.__class__.__name__.replace('Customizer', '')
        return re.sub(r'(?<!^)(?=[A-Z])', '_', cls_name).lower()

    def generate_attribute_prefix(self, attrib):
        return f"{self.prefix}_{attrib}"

    def set_attribute(self, attrib, value):
        setattr(self, self.generate_attribute_prefix(attrib=attrib), value)

    def set_function_prefixes(self) -> None:
        for func in dir(self):
            if (callable(getattr(self, func))) and not (re.match(r'_+.*', func)):
                if not hasattr(self, self.generate_attribute_prefix(attrib=func)):
                    setattr(self, self.generate_attribute_prefix(attrib=func), getattr(self, func))

