import os
import zipfile
import httpx
import asyncio
import aiofiles

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

async def download_and_extract(session, url, download_dir):
    async with session.stream("GET", url) as response:
        filename = os.path.basename(url)
        file_path = os.path.join(download_dir, filename)

        async with aiofiles.open(file_path, "wb") as f:
            async for chunk in response.aiter_bytes():
                await f.write(chunk)

        file_size = os.path.getsize(file_path)
        print(f"Downloaded: {filename}, Size: {file_size} bytes")

        if file_size == 0:
            print(f"Skipping extraction for {filename} as the file size is 0.")
            os.remove(file_path)
            return

        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(download_dir)
            os.remove(file_path)
            print(f"Downloaded and extracted: {filename}")
        except zipfile.BadZipFile:
            print(f"Skipping extraction for {filename} as it is not a valid zip file.")

async def main():
    download_dir = "downloads"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    async with httpx.AsyncClient() as session:
        tasks = [download_and_extract(session, url, download_dir) for url in download_uris]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
