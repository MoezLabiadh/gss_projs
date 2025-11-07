import sys
import os
import importlib
from typing import Iterable, List, Tuple

def _norm(p: str) -> str:
    return os.path.normcase(os.path.normpath(p))

class CloneImporter:
    """
    Load selected packages from one or more cloned site-packages folders while
    running inside ArcGIS Pro’s default Python (arcgispro-py3). Useful in GTS
    when you want newer/custom wheels (e.g., GeoPandas/Shapely/Fiona/PyProj,
    cx_Oracle) without modifying Pro’s base environment.

    What this does
    --------------
    - Prepends clone site-packages paths to `sys.path` for the current process.
    - For each requested package, also prepends its package folder and any
      adjacent “.libs” folder (e.g., shapely.libs) so bundled DLLs load on Windows.
    - Invalidates import caches, then (optionally) checks imports and reports results.

    Priority rules
    --------------
    - Earlier entries in `site_paths` have higher priority. Because we prepend,
      the first matching package found in those paths will be imported before
      the version bundled with arcgispro-py3.

    Scope and safety
    ----------------
    - In-memory only; no conda edits, no Pro environment changes.
    - Reversible: stop using it, and Pro falls back to its own packages.
    - Avoid mixing binary stacks from multiple clones (GDAL/GEOS/PROJ must align).
      Prefer a single consistent clone for compiled geo packages.

    When to use
    -----------
    - Need a newer/fixed wheel than Pro provides.
    - Central IT/Dev maintains “golden” clones for teams; scripts should point to them.
    - CI or dev runs that must not mutate the base Pro environment.

    Typical setup
    -------------
    site_paths:
      P:\\corp\\central_clones\\python_geopandas\\Lib\\site-packages
      P:\\corp\\central_clones\\python_geospatial\\Lib\\site-packages
    packages:
      geopandas, shapely, fiona, pyproj, cx_Oracle

    Example
    -------
    loader = CloneImporter(
        site_paths=(
            r"P:\\corp\\central_clones\\python_geopandas\\Lib\\site-packages",
            r"P:\\corp\\central_clones\\python_geospatial\\Lib\\site-packages",
        ),
        packages=("geopandas", "shapely", "fiona", "pyproj", "cx_Oracle"),
        verbose=True,
    )
    loader.add_paths().import_check().report()
    """

    def __init__(
        self,
        site_paths: Iterable[str] = (
            r"P:\corp\central_clones\python_geopandas\Lib\site-packages",
            r"P:\corp\central_clones\python_geospatial\Lib\site-packages",
        ),
        packages: Iterable[str] = (
            "geopandas", "shapely", "fiona", "pyproj", "cx_Oracle"
        ),
        verbose: bool = True,
    ):
        """
        Initialize with one or more clone paths and the package names you want
        to prioritize from those clones.
        """
        self.site_paths = [p for p in site_paths if p]
        self.packages = list(packages)
        self.verbose = verbose
        self.added_paths: List[str] = []
        self.import_results: List[Tuple[str, str]] = []

    def _prepend_unique(self, paths: Iterable[str]) -> None:
        """Prepend paths to sys.path, de-duplicated with case-insensitive matching."""
        seen = {_norm(p) for p in sys.path}
        for p in reversed(list(paths)):  # reverse so earlier items win
            if not p:
                continue
            pn = _norm(p)
            if pn not in seen:
                sys.path.insert(0, p)
                self.added_paths.append(p)
                seen.add(pn)

    def add_paths(self) -> "CloneImporter":
        """
        Add clone site-packages plus per-package folders (and *.libs when present).
        Call before importing the target packages.
        """
        valid_sites = [sp for sp in self.site_paths if os.path.isdir(sp)]
        self._prepend_unique(valid_sites)

        for sp in valid_sites:
            for pkg in self.packages:
                pkg_dir = os.path.join(sp, pkg)
                if os.path.isdir(pkg_dir):
                    self._prepend_unique([pkg_dir])
                libs_dir = os.path.join(sp, f"{pkg}.libs")
                if os.path.isdir(libs_dir):
                    self._prepend_unique([libs_dir])

        importlib.invalidate_caches()
        return self

    def import_check(self) -> "CloneImporter":
        """
        Try importing each requested package; store ('OK' or error message) for reporting.
        """
        self.import_results.clear()
        for pkg in self.packages:
            try:
                importlib.import_module(pkg)
                self.import_results.append((pkg, "OK"))
            except Exception as exc:
                self.import_results.append((pkg, f"ERROR: {exc.__class__.__name__}: {exc}"))
        return self

    def report(self) -> None:
        """Print what was added to sys.path and the import test results."""
        if not self.verbose:
            return
        print("\nAdded to sys.path (highest priority first):")
        for p in self.added_paths:
            print("  ", p)
        print("\nImport check:")
        for pkg, status in self.import_results:
            print(f"  {pkg:<10} {status}")
        print()

if __name__ == "__main__":
    loader = CloneImporter(
        site_paths=(
            r"P:\corp\central_clones\python_geopandas\Lib\site-packages",
            r"P:\corp\central_clones\python_geospatial\Lib\site-packages",
        ),
        packages=("geopandas", "shapely", "fiona", "pyproj", "cx_Oracle"),
        verbose=True,
    )
    loader.add_paths().import_check().report()

    # Optional: fail fast where needed
    import geopandas, shapely, fiona, pyproj  # noqa: F401
    try:
        import cx_Oracle  # noqa: F401
    except Exception:
        pass
