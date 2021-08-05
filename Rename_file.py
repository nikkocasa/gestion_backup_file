# Pythono3 code to rename multiple
# files in a directory or folder

# importing os module
import os
from shutil import copyfile

# Function to rename multiple files
def main():
    os.chdir("/home/nicolas/Team Drives/direction@es-natura.com/Administration/FINANCES/COMPTABILITE/LABORATOIRE SEDAROME/FOURNISSEURS/FACTURES FOURNISSEURS/factureS Adobe et jetbrain VIA T.N.FARRIE")
    dir_files = os.listdir()
    for filename in [f for f in dir_files if f[0:5] == "Adobe"]:
        fnslpit = filename.split('.')
        date = fnslpit[0].split('_')[-1][2:]
        prefix = 'F' + date + '_'
        dst = prefix + filename

        # rename() function will
        # rename all the files
        copyfile(filename, dst)

    # Driver Code


if __name__ == '__main__':
    # Calling main() function
    main()
