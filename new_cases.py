import requests
import time
import pandas as pd
import sys

def download_new_data(data_csv, state_name):
    #headers = {"Authorization": "Token bdda26a414d35d0f53ac36a26a60bed7c46f8c50"}  

    last_save_time = time.time()
    save_interval = 3600  # 1 hour in seconds

    data = pd.read_csv(data_csv)

    for i, row in data.iterrows():
        # ✅ Skip if already downloaded successfully
        if row["is_downloaded"] == 1 and row["error"] == 0:
            continue

        url = row["courtlistener_url"]
        case_name = url.rstrip("/").split("/")[-1]
        filename = f"/projectnb/sachgrp/apgupta/Case Law Data/CL Data 2020 - 2025/pdfs/{state_name}/{case_name}"

        try:
            print(f"⬇️ Downloading {url} -> {filename}")
            response = requests.get(url, stream=True, timeout=15)  #headers=headers

            if response.status_code == 200:
                # Save file in chunks
                with open(filename, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                data.at[i, "is_downloaded"] = 1
                data.at[i, "error"] = 0

            elif response.status_code == 429:
                print("⏳ Rate limit hit. Waiting 3 minutes before retry...")
                time.sleep(180)  # wait 3 minutes
                # retry once
                retry = requests.get(url, stream=True, timeout=15, headers=headers)
                if retry.status_code == 200:
                    with open(filename, "wb") as f:
                        for chunk in retry.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    data.at[i, "is_downloaded"] = 1
                    data.at[i, "error"] = 0
                else:
                    data.at[i, "error"] = retry.status_code
                    print(f"❌ Still failed after retry ({retry.status_code})")

            elif response.status_code == 404:
                data.at[i, "error"] = 404
                print(f"❌ Not found {url}, skipping.")

            else:
                data.at[i, "error"] = response.status_code
                print(f"❌ Error {response.status_code}")

        except Exception as e:
            print(f"❌ Exception for {url}: {e}")
            data.at[i, "error"] = -1  # custom code for unexpected errors

        # ⏳ Save progress every 1 hour
        if time.time() - last_save_time > save_interval:
            data.to_csv(data_csv, index=False)
            print("💾 Progress saved to CSV")
            last_save_time = time.time()

        time.sleep(1)

    # Final save
    data.to_csv(data_csv, index=False)
    print("\n✅ Run complete")

if __name__ == "__main__":
    download_new_data(sys.argv[1], sys.argv[2]) #pass the .csv file and the state name