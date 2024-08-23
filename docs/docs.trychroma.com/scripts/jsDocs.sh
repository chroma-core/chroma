# cleanup old pages
rm pages/reference/js-client.md
rm pages/reference/js-collection.md

# generate the docs
npx typedoc --disableSources --hideBreadcrumbs true --hideInPageTOC true

# move the generated docs to the correct location
cd ./pages/reference

mv classes/ChromaClient.md js-client.md 
mv classes/Collection.md js-collection.md 



FILE="js-client.md"
TEMP_FILE=$(mktemp)
cat <<- EOF > $TEMP_FILE
---
title: JS Client
---

EOF
cat $FILE >> $TEMP_FILE
mv $TEMP_FILE $FILE


FILE="js-collection.md"
TEMP_FILE=$(mktemp)
cat <<- EOF > $TEMP_FILE
---
title: JS Collection
---

EOF
cat $FILE >> $TEMP_FILE
mv $TEMP_FILE $FILE


# cleanup extra generation files we dont need
rm README.md
rm modules.md
rm -rf interfaces
rm -rf classes