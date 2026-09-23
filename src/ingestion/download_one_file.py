import requests

date = "2024-02-01"

for hour in range(3):

    url = f"https://data.gharchive.org/{date}-{hour}.json.gz"

    output_file = f"data/{date}-{hour}.json.gz"

    print(f"Downloading hour {hour}...")

    response = requests.get(url)

    with open(output_file, "wb") as file:
        file.write(response.content)

    print(f"Finished hour {hour}")

print("All downloads finished!")