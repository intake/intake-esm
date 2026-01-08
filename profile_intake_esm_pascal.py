from memory_profiler import profile

from intake_esm.core import esm_datastore


@profile
def main():
    _ = 1
    cat = esm_datastore('/Users/u1166368/scratch/simulation.json')
    print("\nSearching for variable='tasmin'...\n")
    scat = cat.search(variable='tasmin')

    _srcs = scat.unique()


if __name__ == '__main__':
    main()
