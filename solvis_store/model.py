import logging
from datetime import datetime as dt

from pynamodb.attributes import NumberAttribute, NumberSetAttribute, UnicodeAttribute
from pynamodb.models import Model

from .cloudwatch import ServerlessMetricWriter
from .config import CLOUDWATCH_APP_NAME, DEPLOYMENT_STAGE, IS_OFFLINE, IS_TESTING, REGION

log = logging.getLogger("pynamodb")
# log.setLevel(logging.DEBUG)

db_metrics = ServerlessMetricWriter(lambda_name=CLOUDWATCH_APP_NAME, metric_name="MethodDuration", resolution=1)


class MetricatedModel(Model):
    @classmethod
    def query(cls, *args, **kwargs):
        t0 = dt.utcnow()
        res = super(MetricatedModel, cls).query(*args, **kwargs)
        t1 = dt.utcnow()
        db_metrics.put_duration(__name__, f'{cls.__name__}.query', t1 - t0)
        return res


class SolutionLocationRadiusRuptureSet(MetricatedModel):
    class Meta:
        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"SolutionLocationRadiusRuptureSet-{DEPLOYMENT_STAGE}"
        region = REGION

    solution_id = UnicodeAttribute(hash_key=True)
    location_radius = UnicodeAttribute(range_key=True)  # eg WLG:10000

    radius = NumberAttribute()
    location = UnicodeAttribute()
    ruptures = NumberSetAttribute()  # Rupture Index,
    rupture_count = NumberAttribute()


class SolutionRupture(MetricatedModel):
    class Meta:
        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"SolutionRupture-{DEPLOYMENT_STAGE}"
        region = REGION

    solution_id = UnicodeAttribute(hash_key=True)
    rupture_index_rk = UnicodeAttribute(range_key=True)

    rupture_index = NumberAttribute()
    magnitude = NumberAttribute()  # Magnitude,
    avg_rake = NumberAttribute()  # Average Rake (degrees),
    area_m2 = NumberAttribute()  # Area (m^2),
    length_m = NumberAttribute()  # Length (m),
    annual_rate = NumberAttribute()  # Annual Rate
    fault_sections = NumberSetAttribute()


class SolutionFaultSection(MetricatedModel):
    class Meta:
        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"SolutionFaultSection-{DEPLOYMENT_STAGE}"
        region = REGION

    solution_id = UnicodeAttribute(hash_key=True)
    section_index_rk = UnicodeAttribute(range_key=True)

    section_index = NumberAttribute()
    fault_name = UnicodeAttribute()
    dip_degree = NumberAttribute()
    rake = NumberAttribute()
    low_depth = NumberAttribute()
    up_depth = NumberAttribute()
    dip_dir = NumberAttribute()
    aseismic_slip_factor = NumberAttribute()
    coupling_coeff = NumberAttribute()
    slip_rate = NumberAttribute()
    parent_id = NumberAttribute()
    parent_name = UnicodeAttribute()
    slip_rate_std_dev = NumberAttribute()
    geometry = UnicodeAttribute()


table_classes = (SolutionLocationRadiusRuptureSet, SolutionRupture, SolutionFaultSection)


def set_local_mode(host="http://localhost:8000"):
    if IS_OFFLINE and not IS_TESTING:
        log.info("Setting tables for local dynamodb instance in offline mode")
        for table in table_classes:
            table.Meta.host = host


def drop_all(*args, **kwargs):
    """
    drop all the tables etc
    """
    log.info("Drop all called")
    for table in table_classes:
        if table.exists():
            table.delete_table()
            log.info(f"deleted table: {table}")


def migrate(*args, **kwargs):
    """
    setup the tables etc

    NB: seamless dynamodDB schema migrations are gonna be interesting
    see https://stackoverflow.com/questions/31301160/change-the-schema-of-a-dynamodb-table-what-is-the-best-recommended-way  # noqa
    """
    log.info("Migrate called")
    for table in table_classes:
        if not table.exists():
            table.create_table(wait=True)
            log.info(f"Migrate created table: {table}")
