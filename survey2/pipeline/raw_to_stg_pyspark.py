# datetime import karke failed file ke liye unique timestamp banaya jata hai.
from datetime import datetime

# Path import karke project ke folders aur files ke paths banaye jate hain.
from pathlib import Path

# shutil import karke files copy aur temporary folders delete kiye jate hain.
import shutil

# urlopen import karke Google Sheet ka CSV internet se download kiya jata hai.
from urllib.request import urlopen

# SparkSession import karke local PySpark session start kiya jata hai.
from pyspark.sql import SparkSession

# Google Sheet ki pehchan ke liye us ka unique ID set kiya jata hai.
SHEET_ID = "1OzDHmE7ts9aI2fmqPUTJYUCLTCGTtHuQBYF2tquoRrg"

# Sheet ke us tab ka ID set kiya jata hai jahan se data lena hai.
SHEET_GID = "0"

# Sheet se aane wale columns ke liye apne naam isi order mein set kiye jate hain.
CUSTOM_COLUMN_NAMES = [
    "timestamp",
    "python_skill",
    "career",
    "future_goal",
    "python_logics",
    "interests",
    "learning_style",
    "expectations",
    "pace",
    "difficulty",
    "balance",
]

# Google Sheets ka CSV export URL banaya jata hai.
CSV_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
    f"/export?format=csv&gid={SHEET_GID}"
)

# Script ki location se project root folder ka path hasil kiya jata hai.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Project ke raw folder ka path tay kiya jata hai.
RAW_FOLDER = PROJECT_ROOT / "raw"

# Project ke error folder ka path tay kiya jata hai.
ERROR_FOLDER = PROJECT_ROOT / "error"

# Final CSV file ka path tay kiya jata hai.
RAW_FILE = RAW_FOLDER / "google_sheet_data.csv"

# Download hone wali asal CSV ke liye temporary file ka path tay kiya jata hai.
TEMPORARY_DOWNLOAD = RAW_FOLDER / "google_sheet_data.download.tmp.csv"

# Spark ke temporary output folder ka path tay kiya jata hai.
TEMPORARY_OUTPUT = RAW_FOLDER / "google_sheet_data.tmp"


# Main function mein Google Sheet se raw CSV tak ka poora pipeline chalta hai.
def main():
    # Zaroorat par raw folder banaya jata hai.
    RAW_FOLDER.mkdir(parents=True, exist_ok=True)

    # Zaroorat par error folder banaya jata hai.
    ERROR_FOLDER.mkdir(parents=True, exist_ok=True)

    # Spark session ko pehle None rakha jata hai taa-ke setup fail ho to cleanup safe rahe.
    spark = None

    # Pipeline ke tamam steps chala kar errors ko error folder mein handle kiya jata hai.
    try:
        # Local machine par chalne wala PySpark session banaya jata hai.
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("SurveyRawToStg")
            .getOrCreate()
        )

        # Google Sheet ka CSV response timeout ke saath download kiya jata hai.
        with urlopen(CSV_URL, timeout=60) as response:
            # Downloaded bytes temporary CSV file mein likhe jate hain.
            with TEMPORARY_DOWNLOAD.open("wb") as downloaded_file:
                # Response ka data temporary file mein copy kiya jata hai.
                shutil.copyfileobj(response, downloaded_file)

        # Spark downloaded CSV ko header ke saath aur tamam values strings ke roop mein parhta hai.
        dataframe = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .option("multiLine", "true")
            .csv(str(TEMPORARY_DOWNLOAD))
        )

        # Custom names di gayi hon to unki tadaad CSV columns se milayi jati hai.
        if CUSTOM_COLUMN_NAMES:
            # Ghalat tadaad par wazeh error uthaya jata hai.
            if len(CUSTOM_COLUMN_NAMES) != len(dataframe.columns):
                # Error mein darkar aur di gayi column names ki tadaad batayi jati hai.
                raise ValueError(
                    f"{len(dataframe.columns)} column names chahiye, "
                    f"lekin {len(CUSTOM_COLUMN_NAMES)} diye gaye."
                )

            # Spark DataFrame ke column labels custom names se badle jate hain.
            dataframe = dataframe.toDF(*CUSTOM_COLUMN_NAMES)

        # DataFrame ki pehli 20 rows terminal par dikhayi jati hain.
        dataframe.show(20, truncate=False)

        # Spark output ko aik partition mein la kar temporary CSV folder mein likhta hai.
        dataframe.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            str(TEMPORARY_OUTPUT)
        )

        # Spark output folder ke andar tayyar CSV part file dhoondi jati hai.
        part_files = list(TEMPORARY_OUTPUT.glob("part-*.csv"))

        # CSV part file na mile to pipeline ko ghalati ke saath roka jata hai.
        if not part_files:
            # Spark output missing hone par wazeh error diya jata hai.
            raise FileNotFoundError("Spark output folder mein CSV part file nahi mili.")

        # Spark ki part file ko final raw CSV ke naam se rakha jata hai.
        part_files[0].replace(RAW_FILE)

        # Spark ke temporary output folder aur side files ko hata diya jata hai.
        shutil.rmtree(TEMPORARY_OUTPUT)

        # Downloaded temporary CSV ko hata diya jata hai kyun ke final file ban chuki hai.
        TEMPORARY_DOWNLOAD.unlink()

        # User ko final CSV ka path bataya jata hai.
        print(f"CSV kamyabi se ban gayi: {RAW_FILE}")

    # Kisi bhi error par timestamped failed CSV ko error folder mein save kiya jata hai.
    except Exception as error:
        # Har failed CSV ke liye unique timestamp banaya jata hai.
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

        # Error folder mein failed CSV ka destination path banaya jata hai.
        failed_file = ERROR_FOLDER / f"google_sheet_data_failed_{timestamp}.csv"

        # Downloaded temporary CSV ho to usay error folder mein move kiya jata hai.
        if TEMPORARY_DOWNLOAD.exists():
            # Failed source CSV ko timestamped error file ke naam se rakha jata hai.
            TEMPORARY_DOWNLOAD.replace(failed_file)

            # User ko failed CSV ka path bataya jata hai.
            print(f"Failed CSV error folder mein move hui: {failed_file}")

        # Adhoora Spark output folder ho to usay safely delete kiya jata hai.
        if TEMPORARY_OUTPUT.exists():
            # Temporary Spark output aur uski side files remove ki jati hain.
            shutil.rmtree(TEMPORARY_OUTPUT, ignore_errors=True)

        # Asal exception dobara uthayi jati hai taa-ke run failure wazeh rahe.
        raise error

    # Pipeline khatam hone par Spark session band kiya jata hai.
    finally:
        # Spark session bana tha to usay stop kiya jata hai.
        if spark is not None:
            # Local Spark resources release kiye jate hain.
            spark.stop()


# File seedha chalayi jaye tabhi pipeline start kiya jata hai.
if __name__ == "__main__":
    # Google Sheet se CSV banane wala pipeline chalaya jata hai.
    main()