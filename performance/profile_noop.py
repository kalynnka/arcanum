"""
Profile script to identify performance bottlenecks in NoOpMateria vs Pure Pydantic.

This script uses cProfile and pstats to analyze where time is spent.
"""

from __future__ import annotations

import cProfile
import pstats
import random
from io import StringIO
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from arcanum.base import BaseTransmuter, validation_context
from arcanum.materia.base import NoOpMateria

# ============================================================================
# CONFIGURATION
# ============================================================================

SEED = 42
ITERATIONS = 1000  # Increase for more accurate profiling
random.seed(SEED)

# ============================================================================
# PURE PYDANTIC MODELS
# ============================================================================


class PureAuthorSimple(BaseModel):
    """Simple author without relationships."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    field: Literal["Physics", "Biology", "Chemistry", "Literature", "History"]


# ============================================================================
# NOOP MATERIA TRANSMUTERS
# ============================================================================

noop_materia = NoOpMateria()


@noop_materia.bless()
class NoopAuthor(BaseTransmuter):
    """NoOp Author transmuter."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = Field(default=None, frozen=True)
    name: str
    field: Literal["Physics", "Biology", "Chemistry", "Literature", "History"]


# ============================================================================
# DATA GENERATORS
# ============================================================================


def generate_simple_author_data(count: int = 100, seed: int = SEED) -> list[dict]:
    """Generate simple author data without relationships."""
    random.seed(seed)
    fields = ["Physics", "Biology", "Chemistry", "Literature", "History"]
    names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
    return [
        {
            "id": i,
            "name": f"{random.choice(names)} {random.randint(100, 999)}",
            "field": random.choice(fields),
        }
        for i in range(count)
    ]


# ============================================================================
# PROFILING FUNCTIONS
# ============================================================================


def profile_pure_pydantic(data: list[dict], iterations: int = ITERATIONS):
    """Profile pure Pydantic model validation."""
    for _ in range(iterations):
        for d in data:
            PureAuthorSimple.model_validate(d)


def profile_noop_transmuter(data: list[dict], iterations: int = ITERATIONS):
    """Profile NoOpMateria transmuter validation."""
    for _ in range(iterations):
        for d in data:
            NoopAuthor.model_validate(d)


def run_profile(func, *args, sort_by: str = "cumulative", top_n: int = 40) -> str:
    """Run profiling on a function and return formatted results."""
    profiler = cProfile.Profile()
    profiler.enable()
    func(*args)
    profiler.disable()

    stream = StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.strip_dirs()
    stats.sort_stats(sort_by)
    stats.print_stats(top_n)
    return stream.getvalue()


def main():
    # Generate test data
    data = generate_simple_author_data(100, seed=SEED)

    print("=" * 80)
    print("PROFILING PURE PYDANTIC MODEL VALIDATION")
    print("=" * 80)
    print(
        f"Validating {len(data)} items x {ITERATIONS} iterations = {len(data) * ITERATIONS} total validations"
    )
    print()

    pydantic_profile = run_profile(profile_pure_pydantic, data)
    print(pydantic_profile)

    print("\n" + "=" * 80)
    print("PROFILING NOOP MATERIA TRANSMUTER VALIDATION")
    print("=" * 80)
    print(
        f"Validating {len(data)} items x {ITERATIONS} iterations = {len(data) * ITERATIONS} total validations"
    )
    print()

    noop_profile = run_profile(profile_noop_transmuter, data)
    print(noop_profile)

    # Now let's profile the key hot spots in detail
    print("\n" + "=" * 80)
    print("DETAILED ANALYSIS: SPECIFIC HOT SPOTS")
    print("=" * 80)

    # Profile __init__ specifically
    print("\n--- NoOpMateria __init__ analysis ---")

    def profile_init_only():
        for _ in range(ITERATIONS):
            for d in data:
                NoopAuthor(**d)

    init_profile = run_profile(profile_init_only, sort_by="cumulative", top_n=30)
    print(init_profile)

    # Profile model_construct
    print("\n--- model_construct comparison ---")

    def profile_pydantic_construct():
        for _ in range(ITERATIONS):
            for d in data:
                PureAuthorSimple.model_construct(**d)

    def profile_noop_construct():
        with validation_context():
            for _ in range(ITERATIONS):
                for d in data:
                    NoopAuthor.model_construct(**d)

    print("\nPure Pydantic model_construct:")
    print(run_profile(profile_pydantic_construct, sort_by="cumulative", top_n=20))

    print("\nNoOp Transmuter model_construct:")
    print(run_profile(profile_noop_construct, sort_by="cumulative", top_n=20))


if __name__ == "__main__":
    main()
