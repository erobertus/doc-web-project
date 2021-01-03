ALL_DR_SEARCH_CHECKMARK = \
    "p$lt$ctl01$pageplaceholder$p$lt$ctl02$CPSO_" \
    "AllDoctorsSearch$chkInactiveDoctors"

CPSONO_FIELD = "p$lt$ctl01$pageplaceholder$p$lt$ctl02$CPSO_" \
               "AllDoctorsSearch$txtCPSONumberGeneral"
DELIM_COMMA = ','
DIV = 'div'
SECT = 'section'
CL_DR_INFO = 'doctor-info'
CL_INFO = 'info'
CL_PRACTICE_LOC = 'practice-location'
CL_ADD_PR_LOC = 'location_details'

CL_SPECIALTIES = 'doctor-detail-section'
ID_SPECIALTIES = 'specialties'

CL_HOSPITALS = CL_SPECIALTIES
ID_HOSPITALS = 'hospital_priv'

C_FNAME = 'first_name'
C_LNAME = 'last_name'
C_MNAME = 'middle_name'
C_CPSO_NO = 'CPSO_no'
REG_STAT_TABLE = 'z847e_MD_reg_statuses'
C_REG_STAT_CODE = 'reg_stat_code'
C_REG_STAT_NAME = 'reg_stat_name'
C_REG_EFF_DATE = 'reg_eff_date'
C_REG_EXP_DATE = 'reg_exp_date'
WEB_MEMBER_STAT = 'MEMBER STATUS'
WEB_CPSO_REG_CL = 'CPSO REGISTRATION CLASS'
WEB_EXPIRY_DATE = 'EXPIRY DATE'

REG_CLASS_TABLE = 'z847e_MD_reg_classes'
C_REG_CLASS_CODE = 'reg_class_code'
C_REG_CLASS_NAME = 'reg_class_name'
C_REG_CLASS_DATE = 'reg_certif_date'

WEB_FRMR_NAME = 'Former Name:'
C_FRMR_NAME = 'former_name'
WEB_GENDER = 'Gender:'
GENDER_TABLE = 'MD_genders'
C_GENDER_CODE = 'gender_code'
C_GENDER_NAME = 'gender_name'
C_MD_GENDER = 'gender'
WEB_LANGUAGES = 'Languages Spoken:'
MD_LANGUAGES = WEB_LANGUAGES
LANGUAGE_TABLE = 'z847e_MD_languages'
C_LANG_CODE = 'lang_code'
C_LANG_NAME = 'lang_name'
WEB_UNIVERSITY = 'Education:'
C_UNIV_CODE = 'univ_code'
C_UNIV_NAME = 'univ_name'
C_GRAD_YEAR = 'grad_year'
UNIV_TABLE = 'z847e_MD_universities'
WEB_DATE_OF_DEATH = 'Date of Death:'
C_DATE_OF_DEATH = 'date_of_death'
WEB_REG_IN_OTHER_JUR = 'Medical Licences in Other Jurisdictions'
MD_REG_JURISDIC = WEB_REG_IN_OTHER_JUR
MD_ADDRESSES = 'Practice Addresses'
REG_JUR_TABLE = 'z847e_MD_reg_jurisdiction'
C_JUR_CODE = 'jur_code'
C_JUR_NAME = 'jur_name'

C_ADDR_PREFIX = 'address_'
C_ADDR_CITY = 'city'
C_ADDR_PROV = 'prov_code'
C_ADDR_POSTAL = 'postal_code'
C_ADDR_COUNTRY = 'country'
C_ADDR_DISTRICT = 'district_no'
C_ADDR_ORDER = 'order_no'
C_ADDR_PHONE_NO = 'phone_no'
C_ADDR_EXT = 'ext_no'
C_ADDR_FAX_NO = 'fax_no'
C_ADDR_COUNTY = 'county'


POSTAL_SEPARATOR = '\xa0'
PHONE_TAG = 'Phone:'
FAX_TAG = 'Fax:'
COUNTY_TAG = 'County:'
E_DISTR_TAG = 'Electoral District:'

CANADA = 'Canada'

SPEC_TABLE = 'z847e_MD_spec_list'
C_SPEC_CODE = 'spec_code'
C_SPEC_NAME = 'spec_name'
C_SPEC_DATE = 'spec_issue_date'

STYPE_TABLE = 'z847e_MD_spec_types'
C_STYPE_CODE = 'spec_type_code'
C_STYPE_NAME = 'spec_type_name'

MD_SPECIALTIES = 'specialties'

HOSP_TABLE = 'z847e_MD_hospitals'
C_HOSP_CODE = 'hosp_code'
C_HOSP_NAME = 'hosp_name'

MD_HOSPITALS = 'hospitals'


WEB2DB_MAP = {PHONE_TAG: C_ADDR_PHONE_NO, FAX_TAG: C_ADDR_FAX_NO,
              E_DISTR_TAG: C_ADDR_DISTRICT, COUNTY_TAG: C_ADDR_COUNTY,
              WEB_DATE_OF_DEATH: C_DATE_OF_DEATH,
              WEB_GENDER: C_MD_GENDER}

TEST_CPSO = 82832
