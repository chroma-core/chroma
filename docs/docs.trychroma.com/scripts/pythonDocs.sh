pydoc-markdown

# Function to remove the block from the given file
remove_block() {
  sed -e '/^---$/,/^---$/d' "$1" > "$1.tmp"
  mv "$1.tmp" "$1"
}

# Remove the block from each file
file1="docs/reference/__init__/__init__.md"
file2="docs/reference/api/__init__.md"
file_out="docs/reference/Client.md"
remove_block $file1
remove_block $file2

# Concatenate the files into three.md
cat > "$file_out" << EOF
---
sidebar_label: Client
title: Client
sidebar_position: 1
---

EOF

cat $file1 >> $file_out
cat $file2 >> $file_out

echo "Files processed successfully!"

rm $file1
rm $file2


new_section2=$(cat <<EOF
---
sidebar_label: Collection
title: Collection
sidebar_position: 2
---
EOF
)

# Escape new lines
new_section2=${new_section2//$'\n'/\\n}

# Define the file
file2="docs/reference/Collection.md"

# Check if file2 exists
if [ ! -f "$file2" ]; then
    echo "$file2 not found!"
    exit 1
fi

# Use sed to replace section
# Create an empty backup file for compatibility with macOS/BSD sed
sed -i.bak -e ':a' -e 'N' -e '$!ba' -e 's/---\n.*\n---/'"$new_section2"'/g' "$file2"

# find all examples of "## " inside Collection.md and replace it with "# "
sed -i.bak -e 's/## /# /g' "$file2"
sed -i.bak -e 's/#### /### /g' "$file2"

sed -i.bak -e 's/## /# /g' "$file_out"
sed -i.bak -e 's/### /## /g' "$file_out"
sed -i.bak -e 's/#### /### /g' "$file_out"

sed -i.bak -e 's/API Objects/Client Methods/g' "$file_out"

# remove @override & @abstractmethod 
sed -i.bak -e '/@override/d' "$file_out"
sed -i.bak -e '/@abstractmethod/d' "$file_out"

# Remove the backup file
rm "${file2}.bak"
rm "${file_out}.bak"