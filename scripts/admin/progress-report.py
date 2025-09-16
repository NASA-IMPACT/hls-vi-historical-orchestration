#!/usr/bin/env python
from pathlib import Path

import awswrangler as wr
import click
import matplotlib.pyplot as plt
import pandas as pd

ENVIRONMENTS = ["dev", "prod"]


def create_report(environment: str) -> pd.DataFrame:
    """Create report data using AWS Athena"""
    if environment not in ENVIRONMENTS:
        raise ValueError(f"Unsupported environment, '{environment}'")

    database = f"hls-vi-historical-logs-{environment}"
    sql = """
    WITH latest_attempt AS (
        SELECT
            MAX_BY(outcome, attempt) AS outcome,
            ARBITRARY(platform) AS platform,
            date_trunc('year', ARBITRARY(acquisition_date)) AS year
        FROM "granule_processing_events"
        GROUP BY
            granule_id
    )
    SELECT platform, outcome, year, count(*) as granule_count
    FROM latest_attempt
    GROUP BY platform, outcome, year
    ORDER BY year DESC, platform, outcome DESC
    """
    return wr.athena.read_sql_query(
        sql=sql,
        database=database,
    )


def create_report_plot(report: pd.DataFrame) -> plt.Figure:
    """Create report plot from report data"""
    # Ensure all states are represented in figure
    years = pd.date_range(report.year.min(), report.year.max(), freq="1YS")
    platforms = ["L30", "S30"]
    outcomes = ["success", "failure"]

    index_cols = ["year", "platform", "outcome"]
    index = pd.MultiIndex.from_product([years, platforms, outcomes])
    report = report.set_index(index_cols).reindex(index).reset_index(names=index_cols)

    # Drop to just successes - failures are too small to see and ephemeral
    success = report[report["outcome"] == "success"]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    data = (
        success.groupby(["year", "platform"])["granule_count"]
        .sum()
        .unstack(["platform"])
    )
    data.plot(
        kind="bar",
        ax=ax,
        legend="topright",
    )

    ax.set_title("HLS-VI Historical Processing Status")
    ax.set_xlabel("Year")
    ax.set_xticklabels(
        data.index.strftime("%Y"),
        rotation=0,
    )
    ax.set_ylabel("Granule Count")

    fig.tight_layout()
    return fig


@click.command()
@click.option(
    "--csv",
    type=click.Path(writable=True, path_type=Path),
    required=True,
    help="Write report data to CSV",
)
@click.option(
    "--plot",
    type=click.Path(writable=True, path_type=Path),
    required=True,
    help="Write report graph to PNG file at this location",
)
@click.option(
    "--environment",
    type=click.Choice(ENVIRONMENTS),
    help="Deployed environment",
    required=True,
    envvar="STAGE",
    show_envvar=True,
)
def progress_report(csv: Path, plot: Path, environment: str):
    """Create progress report data and chart"""
    click.echo("Generating report from Athena query...")
    report = create_report(environment=environment)
    click.echo("Creating figure...")
    fig = create_report_plot(report)
    click.echo("Saving...")
    report.to_csv(csv, index=False)
    fig.savefig(plot)
    click.echo("Complete!")


if __name__ == "__main__":
    progress_report()
