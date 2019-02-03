"""
Index page presenting the list of available servers.
"""

from powa.ui_modules import MenuEntry
from powa.dashboards import (
    Dashboard, Grid,
    MetricGroupDef, MetricDef,
    DashboardPage)

from powa.server import ServerOverview
try:
    from collections import OrderedDict
except:
    from ordereddict import OrderedDict


class OverviewMetricGroup(MetricGroupDef):
    """
    Metric group used by the "all servers" grid
    """
    name = "all_servers"
    xaxis = "srvid"
    axis_type = "category"
    data_url = r"/server/all_servers/"
    port = MetricDef(label="Port", type="text")

    @property
    def query(self):

        return ("""SELECT id AS srvid,
                CASE WHEN id = 0 THEN '%(host)s' ELSE hostname END as hostname,
                CASE WHEN id = 0 THEN '%(port)s' ELSE port END AS port
                FROM powa_servers""" % {'host': self.current_host,
                                        'port': self.current_port})

    def process(self, val, **kwargs):
        val = dict(val)
        val["url"] = self.reverse_url("ServerOverview", val["srvid"])
        return val


class Overview(DashboardPage):
    """
    Overview dashboard page.
    """
    base_url = r"/server/"
    datasources = [OverviewMetricGroup]
    title = 'All servers'

    def dashboard(self):
        # This COULD be initialized in the constructor, but tornado < 3 doesn't
        # call it
        if getattr(self, '_dashboard', None) is not None:
            return self._dashboard

        dashes = [[Grid("All servers",
                        columns=[{
                            "name": "hostname",
                            "label": "Host name",
                            "url_attr": "url",
                            "direction": "descending"
                        }],
                        metrics=OverviewMetricGroup.all())]]

        self._dashboard = Dashboard("All servers", dashes)
        return self._dashboard

    @classmethod
    def get_childmenu(cls, handler, params):
        children = []
        for s in list(handler.servers):
            new_params = params.copy()
            new_params["server"] = s[0]
            entry = ServerOverview.get_selfmenu(handler, new_params)
            entry.title = s[1]
            children.append(entry)
        return children
