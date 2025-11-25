#https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes18/l1b/
#Libraries and Dependencies
!pip install netCDF4 s3fs pyarrow fastparquet polars
import s3fs
import xarray as xr
import pandas as pd
import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import gc, os, tempfile

#Download and preprocess Raw Data
fs = s3fs.S3FileSystem(anon=True)

#Create parquete directories
os.makedirs("parquet/mag", exist_ok=True)
os.makedirs("parquet/euv", exist_ok=True)

def load_netcdf_from_s3(path):
    """
    Loads a NetCDF file from NOAA S3 in a Colab-safe, memory-safe way.
    """
    raw = fs.open(path, "rb").read()

    tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
    tmp.write(raw)
    tmp.flush()
    tmp.close()

    ds = xr.open_dataset(tmp.name, engine="netcdf4", chunks={})
    return ds, tmp.name   # return file path so we can delete later


def process_mag_dataset(ds):
    required = ["IB_time", "IB_mag_ECI_uncorrected"]
    ds_sel = ds[required]
    df = ds_sel.to_dataframe().reset_index(drop=True)
    df["time"] = pd.to_datetime(df["IB_time"], unit="ns", errors="coerce")
    return pd.DataFrame({
        "time": df["time"],
        "Bx": df["IB_mag_ECI_uncorrected"].apply(lambda v: v[0] if isinstance(v, np.ndarray) else np.nan),
        "By": df["IB_mag_ECI_uncorrected"].apply(lambda v: v[1] if isinstance(v, np.ndarray) else np.nan),
        "Bz": df["IB_mag_ECI_uncorrected"].apply(lambda v: v[2] if isinstance(v, np.ndarray) else np.nan),
    })


def process_euv_dataset(ds):
    required = ["time","avgIrradiance256","avgIrradiance284","avgIrradiance304","avgIrradiance1175","avgIrradiance1216",
        "avgIrradiance1335","avgIrradiance1405","avgIrradianceXRSA", "avgIrradianceXRSB"]
    ds_sel = ds[required]
    df = ds_sel.to_dataframe().reset_index(drop=True)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return pd.DataFrame({
        "time": df["time"],
        "EUV256": df.get("avgIrradiance256", pd.Series([np.nan])).iloc[0],
        "EUV284": df.get("avgIrradiance284", pd.Series([np.nan])).iloc[0],
        "EUV304": df.get("avgIrradiance304", pd.Series([np.nan])).iloc[0],
        "EUV1175": df.get("avgIrradiance1175", pd.Series([np.nan])).iloc[0],
        "EUV1216": df.get("avgIrradiance1216", pd.Series([np.nan])).iloc[0],
        "EUV1335": df.get("avgIrradiance1335", pd.Series([np.nan])).iloc[0],
        "EUV1405": df.get("avgIrradiance1405", pd.Series([np.nan])).iloc[0],
        "MGINDX": df.get("avgRatioMgExis", pd.Series([np.nan])).iloc[0],
    })

def save_parquet(df, folder, year, day, hour, idx):
    os.makedirs(folder, exist_ok=True)
    out_path = f"{folder}/{year}_{day}_{hour}_{idx}.parquet"
    df.to_parquet(out_path, index=False)
    print("Saved:", out_path)

#TODO: Need to work on code extracting whole data base
sat = "noaa-goes18"
mag_tool = "MAG-L1b-GEOF"
euv_tool = "EXIS-L1b-SFEU"

year = "2024"
day = "001"
hours = ["00"]   # just one hour for testing

for hour in hours:

    mag_files = fs.glob(f"{sat}/{mag_tool}/{year}/{day}/{hour}/*.nc")
    euv_files = fs.glob(f"{sat}/{euv_tool}/{year}/{day}/{hour}/*.nc")

    print(hour, "MAG:", len(mag_files), "EUV:", len(euv_files))

    # -------------------- MAG --------------------
    for i, f in enumerate(mag_files):
        try:
            ds, tmp_path = load_netcdf_from_s3(f)
            df_mag = process_mag_dataset(ds)
            save_parquet(df_mag, "parquet/mag", year, day, hour, i)

        except Exception as e:
            print("MAG error:", f, e)

        finally:
            ds.close()
            os.remove(tmp_path)
            del ds, df_mag
            gc.collect()

    # -------------------- EUV --------------------
    for j, f in enumerate(euv_files):
        try:
            ds, tmp_path = load_netcdf_from_s3(f)
            df_euv = process_euv_dataset(ds)
            save_parquet(df_euv, "parquet/euv", year, day, hour, j)

        except Exception as e:
            print("EUV error:", f, e)

        finally:
            ds.close()
            os.remove(tmp_path)
            del ds, df_euv
            gc.collect()