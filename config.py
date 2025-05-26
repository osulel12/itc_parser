import os
from dotenv import load_dotenv

dotenv_path = '.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

conf_dict = {'authorization_form': 'ctl00_MenuControl_Label_Login',
             'user_form_field': 'Username',
             'pass_form_field': 'Password',
             'mail_user': os.getenv('ITC_LOGIN'),
             'pswd_user': os.getenv('ITC_PASSWORD'),
             'login_button': 'button',
             'type_button_import': 'ctl00_PageContent_label_RadioButton_TradeType_Import',
             'type_button_export': 'ctl00_PageContent_label_RadioButton_TradeType_Export',
             'type_button_Yearly_Time_Series': 'ctl00_PageContent_Button_TimeSeries',
             'trade_type': 'ctl00_NavigationControl_DropDownList_TradeType',
             'output_type': 'ctl00_NavigationControl_DropDownList_OutputType',
             'output_option': 'ctl00_NavigationControl_DropDownList_OutputOption',
             'product_cluster_level': 'ctl00_NavigationControl_DropDownList_ProductClusterLevel',
             'ts_indicator': 'ctl00_NavigationControl_DropDownList_TS_Indicator',
             'ts_currency': 'ctl00_NavigationControl_DropDownList_TS_Currency',
             'num_time_period': 'ctl00_PageContent_GridViewPanelControl_DropDownList_NumTimePeriod',
             'quantity_unit': 'ctl00_NavigationControl_DropDownList_QuantityUnit',
             'country_reporter': 'ctl00_NavigationControl_DropDownList_Country',
             'year_mirror_xpath': '//table[@id="ctl00_PageContent_MyGridView1"]//tr[2]',
             'value_mirror_xpath': '//table[@id="ctl00_PageContent_MyGridView1"]//tr[3]',
             'country_partner': 'ctl00_NavigationControl_DropDownList_Partner',
             'label_login': 'ctl00_MenuControl_Label_Login',
             'world_upper_border': 'ctl00_PageContent_MyGridView1_ctl28_HeaderColspan_Partner',
             'country_upper_border': 'ctl00_PageContent_MyGridView1_ctl28_HeaderColspan_Bilateral',
             'zero_check': '//table[@id="ctl00_PageContent_MyGridView1"]//tr[4]',
             'print_file_name_tl': 'Файл Trade_Map_-_Bilateral_trade_between_{}_and_{}.txt скачен',
             'download_button_txt': 'ctl00_PageContent_GridViewPanelControl_ImageButton_Text',
             'button_back': 'ctl00_PageContent_Button_Back',
             'delete_product': 'ctl00_PageContent_Image_deleteProduct',
             'box_product': 'ctl00_PageContent_RadComboBox_Product',
             'product_drop_down_placeholder': 'ctl00_PageContent_RadComboBox_Product_DropDownPlaceholder',
             'product_drop_down': 'ctl00_PageContent_RadComboBox_Product_DropDown',
             'product_c0': 'ctl00_PageContent_RadComboBox_Product_c0',
             'delete_country': 'ctl00_PageContent_Image_DeleteCountry',
             'box_country_input': 'ctl00_PageContent_RadComboBox_Country_Input',
             'country_drop_down_placeholder': 'ctl00_PageContent_RadComboBox_Country_DropDownPlaceholder',
             'country_drop_down': 'ctl00_PageContent_RadComboBox_Country_DropDown'
             }
