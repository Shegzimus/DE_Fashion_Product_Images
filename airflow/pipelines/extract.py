from PIL import Image, ExifTags
import pandas as pd
import os

# Function to extract metadata from a single image
def extract_metadata(image_path):
    image = Image.open(image_path)
    exif_data = image._getexif()  # Extract EXIF data
    
    # Map EXIF data to readable tags
    metadata = {}
    if exif_data:
        for tag, value in exif_data.items():
            tag_name = ExifTags.TAGS.get(tag, tag)
            metadata[tag_name] = value
    metadata['filename'] = os.path.basename(image_path)  # Add filename for reference
    return metadata

# Directory containing images
image_directory = 'pipelines'
image_files = [os.path.join(image_directory, f) for f in os.listdir(image_directory) if f.lower().endswith('.jpg')]

# Extract metadata for each image and store in a list
metadata_list = [extract_metadata(image_file) for image_file in image_files]

# Convert the list of metadata dictionaries into a DataFrame
df_metadata = pd.DataFrame(metadata_list)

# Save the DataFrame to a CSV file
output_csv_path = 'image_metadata.csv'
df_metadata.to_csv(output_csv_path, index=False)

print(f"Metadata saved to {output_csv_path}")
