rm -r data/03_primary/catwise/*
rm -r data/02_intermediate/catwise.csv
cd data/02_intermediate/
find . -type f -name '*.csv' -exec cat {} + >> catwise.csv
cd ..
mv 02_intermediate/catwise.csv 03_primary/