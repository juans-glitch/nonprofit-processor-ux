# app.py
# A desktop GUI application for processing nonprofit 990 tax filing data.

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lxml import etree
from io import StringIO
import concurrent.futures
import threading
import queue
from datetime import datetime

# --- Core Data Processing Logic (Adapted from main.py) ---
# This "engine" of the application is unchanged.
def process_single_filing(row, progress_queue):
    ein = str(row['ein']).strip().replace('-', '')
    year = str(row['year']).strip()
    try:
        object_id = get_object_id_from_propublica_website(ein, year)
        if object_id:
            download_url = f"https://projects.propublica.org/nonprofits/download-xml?object_id={object_id}"
            response = requests.get(download_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
            response.raise_for_status()
            extracted_record = parse_xml_data(response.content)
            if extracted_record:
                progress_queue.put(f"Success: EIN {ein}, Year {year}")
                return extracted_record
    except requests.exceptions.RequestException as e:
        progress_queue.put(f"Warning: Download failed for EIN {ein}. Reason: {e}")
    except Exception as e:
        progress_queue.put(f"Warning: An unexpected error occurred for EIN {ein}. Reason: {e}")
    progress_queue.put(f"Failed: EIN {ein}, Year {year}")
    return None

def get_object_id_from_propublica_website(ein, year):
    search_url = f"https://projects.propublica.org/nonprofits/organizations/{ein}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(search_url, headers=headers, timeout=20)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    all_links = soup.find_all('a')
    all_object_ids = [link['href'].split('object_id=')[1].strip() for link in all_links if 'href' in link.attrs and 'download-xml?object_id=' in link['href']]
    if not all_object_ids: return None
    try:
        filing_year_prefix = str(int(year) + 1)
        for oid in all_object_ids:
            if oid.startswith(filing_year_prefix): return oid
    except ValueError:
        return None
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

        # This is the full dictionary of XPaths from the original script
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
            'PrepaidExpensesDefrdChargesGrp': './/irs:IRS990/irs:PrepaidExpensesDefrdChargesGrp/irs:EOYAmt',
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
        # --- CORRECTED LOOP ---
        # This loop correctly handles dictionary values that are either 
        # a single string or a list of strings.
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
        # This print statement is helpful for debugging parsing errors.
        print(f"  -> Error parsing an XML file: {e}")
        return None

# --- Main GUI Application Class ---
class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Nonprofit 990 Data Processor")
        self.root.geometry("700x550")
        self.root.configure(bg="#f4f7f9") # Set window background color

        self.input_file_path = None
        self.progress_queue = queue.Queue()

        # --- Style Configuration ---
        style = ttk.Style(self.root)
        style.theme_use('clam') # Use a modern theme
        
        # Configure styles for different widgets
        style.configure('TFrame', background='white')
        style.configure('TLabel', background='white', font=('Helvetica', 12))
        style.configure('Header.TLabel', font=('Helvetica', 22, 'bold'))
        style.configure('Info.TLabel', foreground='#666')
        style.configure('Status.TLabel', font=('Helvetica', 10), background='#f4f7f9')
        style.configure('Accent.TButton', font=('Helvetica', 12, 'bold'), background='#007bff', foreground='white')
        style.map('Accent.TButton', background=[('active', '#0056b3'), ('disabled', '#c0c8d1')])

        # --- Create and layout GUI widgets ---
        self.container = ttk.Frame(self.root, padding=40, style='TFrame')
        self.container.pack(expand=True, padx=20, pady=20)

        # --- Header ---
        header_label = ttk.Label(self.container, text="Nonprofit 990 Data Processor", style='Header.TLabel')
        header_label.pack(pady=(0, 10))

        info_label = ttk.Label(self.container, text="Upload a CSV file with 'ein' and 'year' columns.", style='Info.TLabel', wraplength=500)
        info_label.pack(pady=(0, 20))

        # --- File Drop Zone (visual only) ---
        self.drop_zone = tk.Frame(self.container, bg="white", bd=2, relief="solid", padx=20, pady=40)
        self.drop_zone.pack(fill="x", expand=True, pady=10)
        
        self.drop_zone_text = ttk.Label(self.drop_zone, text="Click here to select a file.", style='TLabel')
        self.drop_zone_text.pack()
        
        # Bind click events to the select_file method
        self.drop_zone.bind("<Button-1>", self.select_file)
        self.drop_zone_text.bind("<Button-1>", self.select_file)

        self.file_label = ttk.Label(self.container, text="", style='Info.TLabel')
        self.file_label.pack(pady=5)
        
        # --- Process Button ---
        self.process_button = ttk.Button(self.container, text="Process Data", style='Accent.TButton', command=self.start_processing_thread, state="disabled")
        self.process_button.pack(fill="x", pady=20, ipady=8)

        # --- Status Bar ---
        self.status_frame = ttk.Frame(self.root, style='Status.TFrame', padding=10)
        self.status_frame.pack(side="bottom", fill="x")

        self.progress = ttk.Progressbar(self.status_frame, orient="horizontal", length=100, mode="determinate")
        self.progress.pack(fill="x", pady=5)
        
        self.status_label = ttk.Label(self.status_frame, text="Ready.", style='Status.TLabel')
        self.status_label.pack(fill="x")
        
        self.root.after(100, self.process_queue)

    def select_file(self, event=None):
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if path:
            self.input_file_path = path
            filename = path.split('/')[-1]
            self.file_label.config(text=f"Selected: {filename}")
            self.process_button.config(state="normal")
            self.status_label.config(text="File selected. Ready to process.")

    def start_processing_thread(self):
        self.process_button.config(state="disabled")
        self.drop_zone.config(relief="solid", bd=1)
        self.progress['value'] = 0
        thread = threading.Thread(target=self.process_data_worker)
        thread.daemon = True
        thread.start()

    def process_data_worker(self):
        try:
            self.progress_queue.put("Reading CSV file...")
            targets_df = pd.read_csv(self.input_file_path, dtype='str')
            MAX_ROWS = 250
            if len(targets_df) > MAX_ROWS:
                raise ValueError(f"Error: File has more than {MAX_ROWS} rows.")
            if 'ein' not in targets_df.columns or 'year' not in targets_df.columns:
                raise ValueError("Error: CSV must contain 'ein' and 'year' columns.")
            
            total_rows = len(targets_df)
            self.progress_queue.put(f"Starting processing for {total_rows} records...")
            all_extracted_data = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_row = {executor.submit(process_single_filing, row, self.progress_queue): row for _, row in targets_df.iterrows()}
                processed_count = 0
                for future in concurrent.futures.as_completed(future_to_row):
                    result = future.result()
                    if result:
                        all_extracted_data.append(result)
                    processed_count += 1
                    self.progress['value'] = (processed_count / total_rows) * 100
            
            if not all_extracted_data:
                raise ValueError("Process finished, but no data could be extracted.")

            self.progress_queue.put("Processing complete. Select a location to save.")
            final_df = pd.DataFrame(all_extracted_data)
            
            date_str = datetime.now().strftime("%Y-%m-%d")
            output_path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV Files", "*.csv")],
                initialfile=f"nonprofit_data_extract_{date_str}.csv",
                title="Save Processed Data As..."
            )
            if output_path:
                final_df.to_csv(output_path, index=False)
                self.progress_queue.put(f"Success! File saved.")
            else:
                self.progress_queue.put("Save cancelled. Process finished.")
        except Exception as e:
            self.progress_queue.put(f"Error: {e}")
        finally:
            self.progress_queue.put("DONE")

    def process_queue(self):
        try:
            message = self.progress_queue.get_nowait()
            if message == "DONE":
                self.process_button.config(state="normal")
                self.drop_zone.config(relief="solid", bd=2)
            else:
                self.status_label.config(text=message)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.process_queue)

# --- Main execution block ---
if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
