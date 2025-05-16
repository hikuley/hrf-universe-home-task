import argparse
from typing import List, Optional, Dict, Tuple

import numpy as np
from sqlalchemy import select, func

from home_task.models import JobPosting, DaysToHireStats
from home_task.db import get_session


def calculate_percentile_stats(values: List[int], min_percentile: float = 10, max_percentile: float = 90) -> tuple:
    """Calculate statistics after filtering by percentiles."""
    if not values:
        return None, None, None, 0
    
    values = np.array(values)
    min_value = np.percentile(values, min_percentile)
    max_value = np.percentile(values, max_percentile)
    
    filtered_values = values[(values >= min_value) & (values <= max_value)]
    
    if len(filtered_values) == 0:
        return None, None, None, 0
    
    return (
        int(np.min(filtered_values)),
        int(np.max(filtered_values)),
        int(np.mean(filtered_values)),
        len(filtered_values)
    )


def get_total_postings_count(session) -> int:
    """Get total number of job postings with valid days_to_hire."""
    return session.execute(
        select(func.count()).select_from(JobPosting).where(JobPosting.days_to_hire.is_not(None))
    ).scalar_one()


def fetch_postings_chunk(session, offset: int, limit: int) -> List[JobPosting]:
    """Fetch a chunk of job postings with pagination."""
    return session.execute(
        select(JobPosting)
        .where(JobPosting.days_to_hire.is_not(None))
        .offset(offset)
        .limit(limit)
    ).scalars().all()


def calculate_stats(session, min_job_postings: int = 5, chunk_size: int = 1000) -> None:
    """Calculate and store days to hire statistics using pagination."""
    total_postings = get_total_postings_count(session)
    stats_by_group: Dict[Tuple[str, Optional[str]], List[int]] = {}
    
    # Process postings in chunks
    for offset in range(0, total_postings, chunk_size):
        job_postings = fetch_postings_chunk(session, offset, chunk_size)
        
        # Group by standard_job_id and country_code
        for posting in job_postings:
            key = (posting.standard_job_id, posting.country_code)
            if key not in stats_by_group:
                stats_by_group[key] = []
            stats_by_group[key].append(posting.days_to_hire)
    
    # Calculate statistics for each group
    for (standard_job_id, country_code), days_to_hire_values in stats_by_group.items():
        min_days, max_days, avg_days, count = calculate_percentile_stats(days_to_hire_values)
        
        if count < min_job_postings:
            continue
        
        # Create or update stats record
        stats = DaysToHireStats(
            id=f"{standard_job_id}_{country_code or 'world'}",
            standard_job_id=standard_job_id,
            country_code=country_code,
            min_days_to_hire=min_days,
            max_days_to_hire=max_days,
            avg_days_to_hire=avg_days,
            job_posting_count=count
        )
        
        session.merge(stats)
    
    session.commit()


def main():
    parser = argparse.ArgumentParser(description="Calculate days to hire statistics")
    parser.add_argument(
        "--min-job-postings",
        type=int,
        default=5,
        help="Minimum number of job postings required to calculate statistics (default: 5)"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Number of job postings to process at once (default: 1000)"
    )
    
    args = parser.parse_args()
    
    session = get_session()
    try:
        calculate_stats(session, args.min_job_postings, args.chunk_size)
    finally:
        session.close()


if __name__ == "__main__":
    main()
