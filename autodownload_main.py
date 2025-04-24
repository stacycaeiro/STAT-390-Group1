import os
import requests

# Where to save the files
download_dir = "power_data_bulk"
os.makedirs(download_dir, exist_ok=True)

# Define files by category
datasets = { 
    "EIA_Form_861": [
        "https://www.eia.gov/electricity/data/eia861/zip/f8612023.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f8612022.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f8612021.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f8612020.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f8612019.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f8612018.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f8612017.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f8612016.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f8612015.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f8612014.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f8612013.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/f8612012.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2011.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2010.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2009.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2008.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2007.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2006.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2005.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2004.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2003.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2002.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2001.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_2000.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_1999.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_1998.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_1997.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_1996.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_1995.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_1994.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_1993.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_1992.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_1991.zip",
        "https://www.eia.gov/electricity/data/eia861/archive/zip/861_1990.zip"
    ],
    "FERC_Form_714": [
        "https://www.ferc.gov/sites/default/files/2020-06/form714-database.zip",
        # 2004
        "https://www.ferc.gov/sites/default/files/2020-09/2004-ecar.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2004-main.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2004-SPP_0.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2004-ercot.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2004-MAPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2004-wecc.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2004-frcc.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2004-NPCC_0.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2004-ASCC-HI.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2004-maac.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2004-serc.zip",
        # 2003
        "https://www.ferc.gov/sites/default/files/2020-09/2003-ecar.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2003-MAPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2003-spp.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2003-ercot.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2003-MAPPONE.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2003-wecc.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2003-frcc.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2003-npcc.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-ASCC3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2003-main.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2003-serc.zip",
        # 2002
        "https://www.ferc.gov/sites/default/files/2020-09/2002-ECAR3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-MAIN3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-SERC3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-ASCC3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-ECARONE3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-MAPP3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-SPP3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-ERCOTONE3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-MAPPONE3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-WECC3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-FRCC3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-NPCC3.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2002-WECCONE3.zip",
        # 2001
        "https://www.ferc.gov/sites/default/files/2020-09/2001-ECAR.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-FRCC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-SERC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-akhi714.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-ecar1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-MAIN.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-SPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-ERCOT.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-MAPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-WECC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-ERCOT1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-NPCC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2001-WSCC.zip",
        # 2000
        "https://www.ferc.gov/sites/default/files/2020-09/2000-ecar.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2000-MAIN.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2000-SPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2000-ERCOT.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2000-MAPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2000-WSCC1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2000-FRCC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2000-NPCC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2000-WSCC2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2000-MAAC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2000-SERC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/2000-OTHER.zip",
        # 1999
        "https://www.ferc.gov/sites/default/files/2020-09/1999-maac.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1999-serc.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1999-OTHER.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1999-ecar.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1999-main.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1999-spp.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1999-ercot.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1999-mapp.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1999-wscc1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1999-frcc.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1999-npcc.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1999-wscc2.zip",      
        # 1998
        "https://www.ferc.gov/sites/default/files/2020-09/1998-maac.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1998-serc.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1998-OTHER.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1998-ecar.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1998-main.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1998-spp.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1998-ercot.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1998-mapp.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1998-wscc1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1998-frcc.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1998-npcc.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1998-wscc2.zip",
        # 1997
        "https://www.ferc.gov/sites/default/files/2020-09/1997-MAAC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-SERC1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-WSCC2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-ECAR.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-MAIN.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-SERC2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-OTHER.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-ERCOT.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-MAPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-SPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-FRCC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-NPCC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1997-WSCC1.zip",        
        # 1996
        "https://www.ferc.gov/sites/default/files/2020-09/1996-MAAC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-SERC1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-WSCC2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-ECAR.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-MAIN.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-SERC2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-OTHER.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-ERCOT.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-MAPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-SPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-FRCC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-NPCC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1996-WSCC1.zip",
        # 1995
        "https://www.ferc.gov/sites/default/files/2020-09/1995-MAAC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1995-SERC1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1995-WSCC2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1995-ECAR.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1995-MAIN.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1995-SERC2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1995-OTHER.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1995-ERCOT.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1995-MAPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1995-SPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1995-NPCC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1995-WSCC1.zip",
        # 1994
        "https://www.ferc.gov/sites/default/files/2020-09/1994-MAAC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-SERC1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-WSCC2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-ECAR.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-MAIN.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-SERC2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-OTHER.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-ERCOT.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-MAPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-SPP1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-SPP2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-NPCC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1994-WSCC1.zip",
        # 1993
        "https://www.ferc.gov/sites/default/files/2020-09/1993-MAAC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-SERC1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-WSCC2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-ECAR.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-MAIN.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-SERC2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-OTHER.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-ERCOT.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-MAPP.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-SPP1.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-SPP2.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-NPCC.zip",
        "https://www.ferc.gov/sites/default/files/2020-09/1993-WSCC1.zip"
    ],
    "EIA_SEDS": [
        "https://www.eia.gov/state/seds/sep_use/total/csv/use_US.csv",
        "https://www.eia.gov/state/seds/sep_prices/total/csv/pr_US.csv",
        "https://www.eia.gov/state/seds/sep_prod/xls/Prod_dataset.xlsx",
        "https://www.eia.gov/state/seds/sep_indicators/energy_indicators.csv",
        "https://www.eia.gov/state/seds/CDF/Complete_SEDS.zip"   
    ],
    "Monthly_Energy_Review": [
        "https://www.eia.gov/totalenergy/data/monthly/Zip_Excel_Month_end/MER_Excel_Zip.zip"
    ],
    "Electric_Power_Monthly": [
        "https://www.eia.gov/electricity/monthly/current_month/march2025.zip"
    ],
    "EPA_FLIGHT_GHGRP": [
        "https://www.epa.gov/system/files/other-files/2024-10/2023_data_summary_spreadsheets.zip",
        "https://www.epa.gov/system/files/other-files/2024-10/ghgp_data_parent_company.xlsb",
        "https://www.epa.gov/system/files/other-files/2024-10/emissions_by_unit_and_fuel_type_c_d_aa.zip",
        "https://www.epa.gov/system/files/other-files/2024-10/l_o_freq_request_data.xlsx",
        "https://www.epa.gov/system/files/other-files/2024-10/e_s_cems_bb_cc_ll_full_data_set.xlsx",
        "https://www.epa.gov/system/files/other-files/2024-10/i_freq_request_data.xlsx"
    ],
    "EPA_NEEDS": [
        "https://www.epa.gov/system/files/documents/2024-08/needs-rev-06-06-2024.xlsx"
    ],
    # EIA Form 860 for the last 10 years
    "EIA_Form_860": [
        f"https://www.eia.gov/electricity/data/eia860/zip/eia860{year}.zip"
        for year in range(2015, 2025)
    ],
    "EPA_eGRID": [
        "https://www.epa.gov/system/files/documents/2025-01/egrid2023_data_rev1.xlsx",
        "https://www.epa.gov/system/files/documents/2024-01/egrid2022_data.xlsx",
        "https://www.epa.gov/system/files/documents/2023-01/eGRID2021_data.xlsx",
        "https://www.epa.gov/system/files/documents/2022-09/eGRID2020_Data_v2.xlsx",
        "https://www.epa.gov/sites/default/files/2021-02/egrid2019_data.xlsx",
        "https://www.epa.gov/sites/default/files/2020-03/egrid2018_data_v2.xlsx",
        "https://www.epa.gov/system/files/other-files/2023-01/egrid_historical_files_1996-2016.zip",
    ],
    "EPA_AVERT_Emission_Rates": [
        "https://www.epa.gov/system/files/documents/2024-04/avert_emission_rates_04-11-24_0.xlsx",
        "https://www.epa.gov/system/files/other-files/2021-09/avert_emission_rates_05-30-19.xlsx"
    ],
    "EIA_CO2_State_Data": [
        "https://www.eia.gov/environment/emissions/state/excel/table1.xlsx",  # Total emissions by year and state
        "https://www.eia.gov/environment/emissions/state/excel/table2.xlsx",  # By fuel type
        "https://www.eia.gov/environment/emissions/state/excel/table3.xlsx",  # By sector
        "https://www.eia.gov/environment/emissions/state/excel/table4.xlsx",  # Electric power sector
        "https://www.eia.gov/environment/emissions/state/excel/table5.xlsx",  # Commercial sector
        "https://www.eia.gov/environment/emissions/state/excel/table6.xlsx",  # Industrial sector
        "https://www.eia.gov/environment/emissions/state/excel/table7.xlsx",  # Residential sector
        "https://www.eia.gov/environment/emissions/state/excel/table8.xlsx"   # Transportation sector
    ],
    "Global_Power_Plant_DB": [
        "https://datasets.wri.org/private-admin/dataset/53623dfd-3df6-4f15-a091-67457cdb571f/resource/66bcdacc-3d0e-46ad-9271-a5a76b1853d2/download/globalpowerplantdatabasev130.zip"
    ],
    "LBL_Utility_Scale_Solar": [
        "https://emp.lbl.gov/sites/default/files/2024-10/Utility-Scale%20Solar%202024%20Edition%20Data%20File.xlsx"
    ],
     "EPA_EIA_Crosswalk": [
        "https://github.com/USEPA/camd-eia-crosswalk/releases/download/v0.3/epa_eia_crosswalk.csv"
    ],
    "RMI_Utility_Transition_Hub": [
        "https://utilitytransitionhub.rmi.org/static/data_download/data_download_all.zip"
    ],
    "EIA_Hourly_Electric_Grid":[
        "https://www.eia.gov/opendata/bulk/EBA-pre2019.zip"
    ],
    "IMF_data_all":[
        "https://www.arcgis.com/sharing/rest/content/items/74a9db2b716d46f2b25c2ec53c0ed1ce/data"
    ],
    "NERC_2024_ESD":[
        "https://www.nerc.com/pa/RAPA/ESD/Documents/2024_ESD.xlsx"
    ]
}

# Download files
for category, urls in datasets.items():
    print(f"\nDownloading: {category}")
    category_dir = os.path.join(download_dir, category)
    os.makedirs(category_dir, exist_ok=True)
    
    for url in urls:
        filename = os.path.join(category_dir, os.path.basename(url))
        try:
            response = requests.get(url)
            response.raise_for_status()
            with open(filename, "wb") as f:
                f.write(response.content)
            print(f"✅ {filename}")
        except Exception as e:
            print(f"❌ Failed to download {url}: {e}")
