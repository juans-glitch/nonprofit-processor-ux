import pandas as pd
import requests
from bs4 import BeautifulSoup
from lxml import etree
import time
from io import StringIO
import os
import base64
import concurrent.futures
from google.cloud import secretmanager

# --- Configuration for Basic Authentication ---
# Fetch the Project ID from the environment variable set by Google Cloud.
PROJECT_ID = os.environ.get('GCP_PROJECT') 
# IMPORTANT: Replace this with the name of the secret you created in Secret Manager.
SECRET_ID = "nonprofit-tool-credentials" 
# This will cache the credentials after the first fetch to improve performance.
CACHED_SECRET = None 

def get_basic_auth_credentials():
    """Fetches and caches the basic auth credentials from Secret Manager."""
    global CACHED_SECRET
    if CACHED_SECRET:
        return CACHED_SECRET

    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        CACHED_SECRET = response.payload.data.decode("UTF-8")
        return CACHED_SECRET
    except Exception as e:
        print(f"FATAL: Could not fetch secret from Secret Manager: {e}")
        # In case of failure, return a dummy value to prevent crashes, but auth will fail.
        return "user:password"

# --- Main Cloud Function Entry Point ---

def process_ein_list(request):
    """
    HTTP Cloud Function that handles all requests.
    - Serves the index.html on GET.
    - Processes CSV on POST after authentication.
    """
    # --- CORRECTED: Define headers at the top ---
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    }

    # Handle CORS preflight request first, as it should not be authenticated.
    if request.method == 'OPTIONS':
        return ('', 204, headers)

    # --- CORRECTED: Basic Authentication Check now includes headers on failure ---
    auth_header = request.headers.get("Authorization")
    expected_credentials = get_basic_auth_credentials()
    
    if auth_header is None:
        # If no auth header, prompt for login, now with CORS headers.
        auth_headers = headers.copy()
        auth_headers['WWW-Authenticate'] = 'Basic realm="Login Required"'
        return ('Unauthorized', 401, auth_headers)

    try:
        encoded_creds = auth_header.split(" ")[1]
        decoded_creds = base64.b64decode(encoded_creds).decode("utf-8")
        if decoded_creds != expected_credentials:
            # On invalid credentials, send error with CORS headers.
            return ('Invalid credentials', 401, headers)
    except Exception:
        # On malformed header, send error with CORS headers.
        return ('Invalid authorization header', 401, headers)
    
    # --- Request Routing ---
    if request.method == 'GET':
        try:
            with open('index.html', 'r') as f:
                html_content = f.read()
            return (html_content, 200, headers)
        except FileNotFoundError:
            return ('Frontend not found.', 500, headers)

    if request.method == 'POST':
        try:
            csv_data = request.data.decode('utf-8')
            targets_df = pd.read_csv(StringIO(csv_data), dtype='str')
            
            MAX_ROWS = 3 
            if len(targets_df) > MAX_ROWS:
                return (f"Too many rows. Please provide a file with no more than {MAX_ROWS} entries.", 413, headers)
            if 'ein' not in targets_df.columns or 'year' not in targets_df.columns:
                return ("Invalid CSV format. Ensure it has 'ein' and 'year' columns.", 400, headers)
            
            print(f"Received {len(targets_df)} records to process.")
        except Exception as e:
            return (f"Invalid CSV data provided: {e}", 400, headers)

        all_extracted_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_row = {executor.submit(process_single_filing, row): row for _, row in targets_df.iterrows()}
            for future in concurrent.futures.as_completed(future_to_row):
                try:
                    result = future.result()
                    if result:
                        all_extracted_data.append(result)
                except Exception as exc:
                    row_info = future_to_row[future]
                    print(f"Row for EIN {row_info.get('ein')} generated an exception: {exc}")

        if not all_extracted_data:
            return ('Process finished, but no data could be extracted from the provided filings.', 404, headers)

        final_df = pd.DataFrame(all_extracted_data)
        leading_cols = ['Ein', 'OrganizationName']
        contractor_cols = sorted([col for col in final_df.columns if col.startswith('Contractor_')])
        other_cols = [col for col in final_df.columns if col not in leading_cols and col not in contractor_cols]
        final_leading_cols = [col for col in leading_cols if col in final_df.columns]
        final_df = final_df[final_leading_cols + contractor_cols + other_cols]

        final_csv_string = final_df.to_csv(index=False)
        response_headers = headers.copy()
        response_headers['Content-Type'] = 'text/csv'
        response_headers['Content-Disposition'] = 'attachment; filename="nonprofit_data_extract.csv"'
        
        print("Process complete. Returning final CSV.")
        return (final_csv_string, 200, response_headers)

    # If method is not GET, POST, or OPTIONS
    return ('Method Not Allowed', 405, headers)

# --- ADDED: New function to process a single row for parallelism ---
def process_single_filing(row):
    """Processes a single EIN/year pair."""
    ein = str(row['ein']).strip().replace('-', '')
    year = str(row['year']).strip()
    print(f"Processing EIN: {ein}, Year: {year}...")

    object_id = get_object_id_from_propublica_website(ein, year)
    
    if object_id:
        download_url = f"https://projects.propublica.org/nonprofits/download-xml?object_id={object_id}"
        try:
            response = requests.get(download_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
            response.raise_for_status()
            return parse_xml_data(response.content)
        except requests.exceptions.RequestException as e:
            print(f"  -> Download failed for {object_id}. Reason: {e}")
    else:
        print(f"  -> No downloadable e-file record found for EIN {ein}, Year {year}.")
    
    return None

# --- Helper Functions ---
def get_object_id_from_propublica_website(ein, year):
    """Scrapes ProPublica by finding all XML links and matching the object_id prefix."""
    print(f"  -> Scraping ProPublica website...")
    search_url = f"https://projects.propublica.org/nonprofits/organizations/{ein}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        response = requests.get(search_url, headers=headers, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        all_links = soup.find_all('a')
        all_object_ids = []
        
        for link in all_links:
            if 'href' in link.attrs and 'download-xml?object_id=' in link['href']:
                object_id = link['href'].split('object_id=')[1].strip()
                all_object_ids.append(object_id)
        
        if not all_object_ids:
            return None

        try:
            filing_year_prefix = str(int(year) + 1)
        except ValueError:
            return None

        for oid in all_object_ids:
            if oid.startswith(filing_year_prefix):
                return oid
        
        return None
    except requests.exceptions.RequestException:
        return None

def parse_xml_data(xml_content):
    """Parses XML content and extracts data using XPath."""
    try:
        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(xml_content, parser)
        ns = {'irs': 'http://www.irs.gov/efile'}

        def get_text(path, context_node=root):
            result = context_node.xpath(path, namespaces=ns)
            return result[0].text.strip() if result and result[0].text else ""

        field_mappings = {
            'Ein': './/irs:Filer/irs:EIN',
            'OrganizationName': './/irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt',
            'WebsiteAddressTxt': './/irs:IRS990/irs:WebsiteAddressTxt',
            'MissionDesc': './/irs:IRS990/irs:MissionDesc',
            'TaxYr': './/irs:ReturnHeader/irs:TaxYr',
            'VotingMembersGoverningBodyCnt': './/irs:IRS990/irs:VotingMembersGoverningBodyCnt',
            'VotingMembersIndependentCnt': './/irs:IRS990/irs:VotingMembersIndependentCnt',
            'FederatedCampaignsAmt': './/irs:IRS990/irs:FederatedCampaignsAmt',
            'MembershipDuesAmt': './/irs:IRS990/irs:MembershipDuesAmt',
            'FundraisingEventsAmt': './/irs:IRS990/irs:FundraisingEventsAmt',
            'RelatedOrganizationsAmt': './/irs:IRS990/irs:RelatedOrganizationsAmt',
            'GovernmentGrantsAmt': './/irs:IRS990/irs:GovernmentGrantsAmt',
            'AllOtherContributionsAmt': './/irs:IRS990/irs:AllOtherContributionsAmt',
            'NonCashContributionsAmt': './/irs:IRS990/irs:NoncashContributionsAmt',
            'TotalContributionsAmt': './/irs:IRS990/irs:TotalContributionsAmt',
            'TotalProgramServiceRevenueAmt': './/irs:IRS990/irs:TotalProgramServiceRevenueAmt',
            'CYInvestmentIncomeAmt': './/irs:IRS990/irs:CYInvestmentIncomeAmt',
            'IncmFromInvestBondProceedsGrp': './/irs:IRS990/irs:IncmFromInvestBondProceedsGrp/irs:TotalRevenueColumnAmt',
            'RoyaltiesAmt': './/irs:IRS990/irs:RoyaltiesRevenueGrp/irs:TotalRevenueColumnAmt',
            'NetRentalIncomeAmt': [
                './/irs:IRS990/irs:RentalIncomeOrLossGrp/irs:RealAmt',
                './/irs:IRS990/irs:RentalIncomeOrLossGrp/irs:PersonalAmt'
            ],
            'NetGainOrLossInvestmentsGrp': './/irs:IRS990/irs:NetGainOrLossInvestmentsGrp/irs:TotalRevenueColumnAmt',
            'NetIncmFromFundraisingEvtGrp': './/irs:IRS990/irs:NetIncmFromFundraisingEvtGrp/irs:TotalRevenueColumnAmt',
            'NetIncomeFromGamingGrp': './/irs:IRS990/irs:NetIncomeFromGamingGrp/irs:TotalRevenueColumnAmt',
            'NetInventorySalesAmt': './/irs:IRS990/irs:NetIncomeOrLossGrp/irs:TotalRevenueColumnAmt',
            'OtherRevenueTotalAmt': './/irs:IRS990/irs:OtherRevenueTotalAmt',
            'TotalRevenue': './/irs:IRS990/irs:TotalRevenueGrp/irs:TotalRevenueColumnAmt',
            'GrantsToDomesticOrgsGrp': './/irs:IRS990/irs:GrantsToDomesticOrgsGrp/irs:TotalAmt',
            'GrantsToDomesticIndividualsGrp': './/irs:IRS990/irs:GrantsToDomesticIndividualsGrp/irs:TotalAmt',
            'ForeignGrantsGrp': './/irs:IRS990/irs:ForeignGrantsGrp/irs:TotalAmt',
            'BenefitsToMembersGrp': './/irs:IRS990/irs:BenefitsToMembersGrp/irs:TotalAmt',
            'CompCurrentOfcrDirectorsGrp': './/irs:IRS990/irs:CompCurrentOfcrDirectorsGrp/irs:TotalAmt',
            'CompDisqualPersonsGrp': './/irs:IRS990/irs:CompDisqualPersonsGrp/irs:TotalAmt',
            'OtherSalariesAndWagesGrp': './/irs:IRS990/irs:OtherSalariesAndWagesGrp/irs:TotalAmt',
            'PensionPlanContributionsGrp': './/irs:IRS990/irs:PensionPlanContributionsGrp/irs:TotalAmt',
            'OtherEmployeeBenefitsGrp': './/irs:IRS990/irs:OtherEmployeeBenefitsGrp/irs:TotalAmt',
            'PayrollTaxesGrp': './/irs:IRS990/irs:PayrollTaxesGrp/irs:TotalAmt',
            'FeesForServicesManagementGrp': './/irs:IRS990/irs:FeesForServicesManagementGrp/irs:TotalAmt',
            'FeesForServicesLegalGrp': './/irs:IRS990/irs:FeesForServicesLegalGrp/irs:TotalAmt',
            'FeesForServicesAccountingGrp': './/irs:IRS990/irs:FeesForServicesAccountingGrp/irs:TotalAmt',
            'FeesForServicesLobbyingGrp': './/irs:IRS990/irs:FeesForServicesLobbyingGrp/irs:TotalAmt',
            'FeesForServicesProfFundraising': './/irs:IRS990/irs:FeesForServicesProfFundraising/irs:TotalAmt',
            'FeesForSrvcInvstMgmntFeesGrp': './/irs:IRS990/irs:FeesForSrvcInvstMgmntFeesGrp/irs:TotalAmt',
            'FeesForServicesOtherGrp': './/irs:IRS990/irs:FeesForServicesOtherGrp/irs:TotalAmt',
            'AdvertisingGrp': './/irs:IRS990/irs:AdvertisingGrp/irs:TotalAmt',
            'OfficeExpensesGrp': './/irs:IRS990/irs:OfficeExpensesGrp/irs:TotalAmt',
            'InformationTechnologyGrp': './/irs:IRS990/irs:InformationTechnologyGrp/irs:TotalAmt',
            'RoyaltiesGrp': './/irs:IRS990/irs:RoyaltiesGrp/irs:TotalAmt',
            'OccupancyGrp': './/irs:IRS990/irs:OccupancyGrp/irs:TotalAmt',
            'TravelGrp': './/irs:IRS990/irs:TravelGrp/irs:TotalAmt',
            'PymtTravelEntrtnmntPubOfclGrp': './/irs:IRS990/irs:PymtTravelEntrtnmntPubOfclGrp/irs:TotalAmt',
            'ConferencesMeetingsGrp': './/irs:IRS990/irs:ConferencesMeetingsGrp/irs:TotalAmt',
            'InterestGrp': './/irs:IRS990/irs:InterestGrp/irs:TotalAmt',
            'PaymentsToAffiliatesGrp': './/irs:IRS990/irs:PaymentsToAffiliatesGrp/irs:TotalAmt',
            'DepreciationDepletionGrp': './/irs:IRS990/irs:DepreciationDepletionGrp/irs:TotalAmt',
            'InsuranceGrp': './/irs:IRS990/irs:InsuranceGrp/irs:TotalAmt',
            'TotalFunctionalExpenseAmt': './/irs:IRS990/irs:TotalFunctionalExpensesGrp/irs:TotalAmt',
            'TotalProgramServiceExpensesAmt': './/irs:IRS990/irs:TotalProgramServiceExpensesAmt',
            'ManagementAndGeneralAmt': './/irs:IRS990/irs:TotalFunctionalExpensesGrp/irs:ManagementAndGeneralAmt',
            'FundraisingAmt': './/irs:IRS990/irs:TotalFunctionalExpensesGrp/irs:FundraisingAmt',
            'CashNonInterestBearingGrp': './/irs:IRS990/irs:CashNonInterestBearingGrp/irs:EOYAmt',
            'SavingsAndTempCashInvstGrp': './/irs:IRS990/irs:SavingsAndTempCashInvstGrp/irs:EOYAmt',
            'PledgesAndGrantsReceivableGrp': './/irs:IRS990/irs:PledgesAndGrantsReceivableGrp/irs:EOYAmt',
            'AccountsReceivableGrp': './/irs:IRS990/irs:AccountsReceivableGrp/irs:EOYAmt',
            'ReceivablesFromOfficersEtcGrp': './/irs:IRS990/irs:ReceivablesFromOfficersEtcGrp/irs:EOYAmt',
            'RcvblFromDisqualifiedPrsnGrp': './/irs:IRS990/irs:RcvblFromDisqualifiedPrsnGrp/irs:EOYAmt',
            'OthNotesLoansReceivableNetGrp': './/irs:IRS990/irs:OthNotesLoansReceivableNetGrp/irs:EOYAmt',
            'InventoriesForSaleOrUseGrp': './/irs:IRS990/irs:InventoriesForSaleOrUseGrp/irs:EOYAmt',
            'PrepaidExpensesDefrdChargesGrp': './/irs:IRS990/prepaidExpensesDefrdChargesGrp/irs:EOYAmt',
            'LandBldgEquipBasisNetGrp': './/irs:IRS990/irs:LandBldgEquipBasisNetGrp/irs:EOYAmt',
            'InvestmentsPubTradedSecGrp': './/irs:IRS990/irs:InvestmentsPubTradedSecGrp/irs:EOYAmt',
            'InvestmentsOtherSecuritiesGrp': './/irs:IRS990/irs:InvestmentsOtherSecuritiesGrp/irs:EOYAmt',
            'InvestmentsProgramRelatedGrp': [
                './/irs:IRS990/irs:InvestmentsProgramRelatedGrp/irs:TotalBookValueProgramRltdAmt',
                './/irs:IRS990/irs:InvestmentsProgramRelatedGrp/irs:EOYAmt'
            ],
            'IntangibleAssetsGrp': './/irs:IRS990/irs:IntangibleAssetsGrp/irs:EOYAmt',
            'OtherAssetsTotalGrp': './/irs:IRS990/irs:OtherAssetsTotalGrp/irs:EOYAmt',
            'TotalAssetsGrp': './/irs:IRS990/irs:TotalAssetsGrp/irs:EOYAmt',
            'AccountsPayableAccrExpnssGrp': './/irs:IRS990/irs:AccountsPayableAccrExpnssGrp/irs:EOYAmt',
            'GrantsPayableGrp': './/irs:IRS990/irs:GrantsPayableGrp/irs:EOYAmt',
            'DeferredRevenueGrp': './/irs:IRS990/irs:DeferredRevenueGrp/irs:EOYAmt',
            'TaxExemptBondLiabilitiesGrp': './/irs:IRS990/irs:TaxExemptBondLiabilitiesGrp/irs:EOYAmt',
            'EscrowAccountLiabilityGrp': './/irs:IRS990/irs:EscrowAccountLiabilityGrp/irs:EOYAmt',
            'LoansFromOfficersDirectorsGrp': './/irs:IRS990/irs:LoansFromOfficersDirectorsGrp/irs:EOYAmt',
            'MortgNotesPyblScrdInvstPropGrp': './/irs:IRS990/irs:MortgNotesPyblScrdInvstPropGrp/irs:EOYAmt',
            'UnsecuredNotesLoansPayableGrp': './/irs:IRS990/irs:UnsecuredNotesLoansPayableGrp/irs:EOYAmt',
            'OtherLiabilitiesGrp': './/irs:IRS990/irs:OtherLiabilitiesGrp/irs:EOYAmt',
            'TotalLiabilitiesGrp': './/irs:IRS990/irs:TotalLiabilitiesGrp/irs:EOYAmt',
            'NoDonorRestrictionNetAssetsGrp': './/irs:IRS990/irs:NoDonorRestrictionNetAssetsGrp/irs:EOYAmt',
            'DonorRestrictionNetAssetsGrp': './/irs:IRS990/irs:DonorRestrictionNetAssetsGrp/irs:EOYAmt',
            'TotalNetAssetsFundBalanceGrp': './/irs:IRS990/irs:TotalNetAssetsFundBalanceGrp/irs:EOYAmt',
            'TotLiabNetAssetsFundBalanceGrp': './/irs:IRS990/irs:TotLiabNetAssetsFundBalanceGrp/irs:EOYAmt',
        }
        
        extracted_data = {}
        for field_name, paths in field_mappings.items():
            value = ""
            path_list = paths if isinstance(paths, list) else [paths]
            for path in path_list:
                value = get_text(path)
                if value:
                    break
            extracted_data[field_name] = value

        MAX_CONTRACTORS = 5
        contractor_nodes = root.xpath('.//irs:IRS990/irs:ContractorCompensationGrp', namespaces=ns)
        
        for i, contractor_node in enumerate(contractor_nodes[:MAX_CONTRACTORS]):
            name = get_text('./irs:ContractorName/irs:PersonNm', contractor_node) or get_text('./irs:ContractorName/irs:BusinessName/irs:BusinessNameLine1Txt', contractor_node)
            services = get_text('./irs:ServicesDesc', contractor_node)
            compensation = get_text('./irs:CompensationAmt', contractor_node)
            
            addr_parts = [
                get_text('./irs:ContractorAddress/irs:USAddress/irs:AddressLine1Txt', contractor_node),
                get_text('./irs:ContractorAddress/irs:USAddress/irs:CityNm', contractor_node),
                get_text('./irs:ContractorAddress/irs:USAddress/irs:StateAbbreviationCd', contractor_node),
                get_text('./irs:ContractorAddress/irs:USAddress/irs:ZIPCd', contractor_node)
            ]
            address = ', '.join(filter(None, addr_parts))

            extracted_data[f'Contractor_{i+1}_Name'] = name
            extracted_data[f'Contractor_{i+1}_Services'] = services
            extracted_data[f'Contractor_{i+1}_Compensation'] = compensation
            extracted_data[f'Contractor_{i+1}_Address'] = address

        for i in range(len(contractor_nodes), MAX_CONTRACTORS):
            extracted_data[f'Contractor_{i+1}_Name'] = ""
            extracted_data[f'Contractor_{i+1}_Services'] = ""
            extracted_data[f'Contractor_{i+1}_Compensation'] = ""
            extracted_data[f'Contractor_{i+1}_Address'] = ""

        return extracted_data
    except Exception as e:
        print(f"  -> Error parsing an XML file: {e}")
        return None
