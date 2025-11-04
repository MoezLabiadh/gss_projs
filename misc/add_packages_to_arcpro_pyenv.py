import sys
import os
import importlib

def add_clone_packages(
    site_path=r"P:\corp\central_clones\python_geopandas\Lib\site-packages",
    packages=None
):
    """
    Extend the default ArcGIS Pro 'arcgispro-py3' environment with selected packages
    from a cloned Python environment (e.g., python_geopandas) when working in GTS.

    This function is used when you’re running scripts in the ArcGIS Pro environment
    (arcgispro-py3) but want to temporarily load more recent or custom builds of 
    packages (such as GeoPandas, Shapely, Fiona, PyProj, or cx_Oracle) from a 
    central clone without altering ArcGIS Pro’s base environment.

    Parameters
    ----------
    site_path : str, optional
        Path to the cloned environment's site-packages directory.
        Default: P:\\corp\\central_clones\\python_geopandas\\Lib\\site-packages
    packages : list[str], optional
        Packages to add from the clone. Defaults to:
        ['geopandas', 'shapely', 'fiona', 'pyproj', 'cx_Oracle']

    Example
    -------
    >>> add_clone_packages()
    >>> import geopandas as gpd
    >>> import cx_Oracle
    """

    if packages is None:
        packages = ["geopandas", "shapely", "fiona", "pyproj", "cx_Oracle"]

    # --- Prioritize the clone path ---
    if site_path in sys.path:
        sys.path.remove(site_path)
    sys.path.insert(0, site_path)

    # --- Add each package (and its .libs folder if found) ---
    for pkg in packages:
        pkg_path = os.path.join(site_path, pkg)
        if os.path.isdir(pkg_path):
            if pkg_path in sys.path:
                sys.path.remove(pkg_path)
            sys.path.insert(0, pkg_path)
            # Add companion .libs (e.g., shapely.libs) for compiled binaries
            libs_path = os.path.join(site_path, f"{pkg}.libs")
            if os.path.isdir(libs_path):
                if libs_path in sys.path:
                    sys.path.remove(libs_path)
                sys.path.insert(0, libs_path)

    importlib.invalidate_caches()

    print("\n✅ Added the following paths to ArcGIS Pro (arcgispro-py3) search path:")
    for p in sys.path[:len(packages) + 3]:
        print("   ", p)
    print("\nPackages from the clone are now prioritized for GTS scripts.\n")


# Example usage:
if __name__ == "__main__":
    add_clone_packages()
    import geopandas, shapely, cx_Oracle
    print("✅ Successfully imported selected packages from clone.")
