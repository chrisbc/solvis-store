from typing import List, Iterator, Dict

import logging
from functools import lru_cache

#from pathlib import PurePath
import solvis

from solvis_store import model

def clean_slate():
    model.drop_all()
    model.migrate()

#@lru_cache(maxsize=32)
def get_location_radius_rupture_models(solution_id:str, sol: solvis.InversionSolution, locations:List[Dict], radii:List[int]) -> Iterator[model.SolutionLocationRadiusRuptureSet]:
    for item in locations:
        print(item)
        for radius in radii:
            polygon = solvis.circle_polygon(radius_m=radius, lat=item['latitude'], lon=item['longitude'])
            rupts = set(sol.get_ruptures_intersecting(polygon).tolist())

            loc = item['id']
            #print(loc, radius, len(rupts))

            if len(rupts) > 1e5:
                raise Exception(f"Too many ruptures in {loc} with radius {radius}: {len(rupts)}")

            yield model.SolutionLocationRadiusRuptureSet(
                location_radius = f"{loc}:{int(radius)}",
                solution_id = solution_id,
                radius = int(radius),
                location = loc,
                ruptures = rupts,
                rupture_count = len(rupts))

#sol, locations, radii
def save_solution_location_radii(solution_id: str, models: List[model.SolutionLocationRadiusRuptureSet]):
    with model.SolutionLocationRadiusRuptureSet.batch_write() as batch:
        for item in models:
            #print(item)
            #item.save()
            batch.save(item)

#@lru_cache(maxsize=32)
def get_ruptures_with_rates(solution_id, sol) -> Iterator[model.SolutionRupture]:
    rs = sol.rupture_sections
    for row in sol.ruptures_with_rates.itertuples():
        sections = [int(x) for x in rs[rs.rupture==int(row[1])].section.tolist()]
        yield model.SolutionRupture(
            solution_id = solution_id,
            rupture_index_rk = str(row[1]),
            rupture_index = int(row[1]),
            magnitude = float(row[2]),     # Magnitude,
            avg_rake = float(row[3]),      # Average Rake (degrees),
            area_m2 = float(row[4]),       # Area (m^2),
            length_m = float(row[5]),      # Length (m),
            annual_rate = float(row[6]),   # Annual Rate
            fault_sections = sorted(sections)
        )

def save_solution_ruptures(solution_id, models: List[model.SolutionRupture]):
    with model.SolutionRupture.batch_write() as batch:
        for item in models:
            batch.save(item)

#@lru_cache(maxsize=32)
def get_fault_section_models(solution_id, sol) -> Iterator[model.SolutionFaultSection]:
    for row in sol.fault_sections.itertuples():
        yield model.SolutionFaultSection(
            solution_id = solution_id,
            section_index_rk = str(row[1]),
            section_index = row[1],
            fault_name = row[2],
            dip_degree = float(row[3]),
            rake = float(row[4]),
            low_depth = float(row[5]),
            up_depth = float(row[6]),
            dip_dir = float(row[7]),
            aseismic_slip_factor = float(row[8]),
            coupling_coeff = float(row[9]),
            slip_rate = float(row[10]),
            parent_id = int(row[11]),
            parent_name = row[12],
            slip_rate_std_dev = float(row[13]),
            geometry = str(row[14])
        )

def save_solution_fault_sections(solution_id, models: List[model.SolutionFaultSection]):
    with model.SolutionFaultSection.batch_write() as batch:
        for item in models:
            batch.save(item)
