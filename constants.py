BATCH_SIZE = 50
USE_RANDOM = False

ALL_DR_SEARCH_CHECKMARK = \
    "p$lt$ctl01$pageplaceholder$p$lt$ctl02$CPSO_" \
    "AllDoctorsSearch$chkInactiveDoctors"

CPSONO_FIELD = "p$lt$ctl01$pageplaceholder$p$lt$ctl02$CPSO_" \
               "AllDoctorsSearch$txtCPSONumberGeneral"
DELIM_COMMA = ','
BLANK = ''
ALL = 'all'
DIV = 'div'
BEGIN_TRAN = 'START TRANSACTION'
COMMIT_TRAN = 'COMMIT'
ABORT_ALL = '!!!ABORT_ALL'
SECT = 'section'
CL_DR_INFO = 'doctor-info'
CL_INFO = 'info'
CL_PRACTICE_LOC = 'practice-location'
CL_ADD_PR_LOC = 'location_details'
CL_ERROR = 'global-error-msg'

CL_SPECIALTIES = 'doctor-detail-section'
ID_SPECIALTIES = 'specialties'

CL_HOSPITALS = CL_SPECIALTIES
ID_HOSPITALS = 'hospital_priv'

MD_DIR_TABLE = 'z847e_MD_dir'

C_FNAME = 'first_name'
C_LNAME = 'last_name'
C_MNAME = 'middle_name'
C_CPSO_NO = 'CPSO_no'
C_CHK_OUT = 'is_checked_out'
C_LAST_MODIF = 'last_modified'
C_FORCE_UPD = 'force_update'

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
C_DEF_ADDR = 'def_address'
WEB_LANGUAGES = 'Languages Spoken:'
MD_LANG_TABLE = 'z847e_MD_doc_x_lang'
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
MD_REG_JURISDIC = 'z847e_MD_doc_x_jurisdiction'
MD_ADDR_TABLE = 'MD_addresses'
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
C_ADDR_IS_DEF = 'isDefault'
C_ADDR_UNO = 'row_uno'
C_GEO_UNO = 'geo_uno'
NO_ADDR = 'Practice Address Not Available'

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

MD_SPEC_TABLE = 'z847e_MD_doc_x_spec'

HOSP_TABLE = 'z847e_MD_hospitals'
C_HOSP_CODE = 'hosp_code'
C_HOSP_NAME = 'hosp_name'

WEB_NO_HOSP = 'No Privileges reported.'
WEB_HOSP_NOTICES = 'Hospital Notices'
MD_HOSP_TABLE = 'z847e_MD_doc_x_hosp'

WEB2DB_MAP = {PHONE_TAG: C_ADDR_PHONE_NO, FAX_TAG: C_ADDR_FAX_NO,
              E_DISTR_TAG: C_ADDR_DISTRICT, COUNTY_TAG: C_ADDR_COUNTY,
              WEB_DATE_OF_DEATH: C_DATE_OF_DEATH,
              WEB_GENDER: C_MD_GENDER}

PREFIX = 'pre'
SUFFIX = 'post'
LABEL = 'label'

PRES_MAP = {C_CPSO_NO: {LABEL: 'CPSO: ', SUFFIX: ':'},
            C_LNAME: {LABEL: 'Name: ', SUFFIX: ','},
            C_ADDR_PREFIX + '1': {LABEL: '\n   Address: '},
            C_ADDR_PREFIX + '2': {PREFIX: ', '},
            C_ADDR_PREFIX + '3': {PREFIX: ', '},
            C_ADDR_PREFIX + '4': {PREFIX: ', '},
            C_ADDR_CITY: {PREFIX: ', '},
            C_ADDR_POSTAL: {PREFIX: '  ', SUFFIX: ','},
            C_FRMR_NAME: {PREFIX: ' A.K.A.: '},
            C_ADDR_PHONE_NO: {PREFIX: ', Phone: '},
            C_ADDR_FAX_NO: {PREFIX: ', Fax: '}
            }

ADDR_MAP = {C_CPSO_NO: {LABEL: 'CPSO: ', SUFFIX: ':'},
            C_LNAME: {LABEL: 'Name: ', SUFFIX: ','},
            C_ADDR_PREFIX + '2': {PREFIX: ', '},
            C_ADDR_PREFIX + '3': {PREFIX: ', '},
            C_ADDR_PREFIX + '4': {PREFIX: ', '},
            C_ADDR_CITY: {PREFIX: ', '},
            C_ADDR_POSTAL: {PREFIX: '  ', SUFFIX: ','},
            C_FRMR_NAME: {PREFIX: ' A.K.A.: '},
            C_ADDR_PHONE_NO: {PREFIX: ', Phone: '},
            C_ADDR_FAX_NO: {PREFIX: ', Fax: '}
            }


T_KEY = 'key'
T_VAL = 'val'
T_FKEY = 'fkey'
T_OWNKEY = 'ownkey'

BATCH_HEAD_TBL = 'MD_batch_header'
BATCH_DET_TBL = 'MD_batch_details'

DB_SCHEMA = {SPEC_TABLE: {T_KEY: C_SPEC_CODE},
             STYPE_TABLE: {T_KEY: C_STYPE_CODE,
                           T_VAL: C_STYPE_NAME},
             HOSP_TABLE: {T_KEY: C_HOSP_CODE, T_VAL: C_HOSP_NAME},
             MD_DIR_TABLE: {T_KEY: C_CPSO_NO},
             MD_LANG_TABLE: {T_KEY: C_CPSO_NO, T_VAL: C_LANG_CODE},
             MD_HOSP_TABLE: {T_KEY: C_CPSO_NO, T_VAL: C_HOSP_CODE},
             MD_ADDR_TABLE: {T_KEY: C_CPSO_NO, T_FKEY: C_DEF_ADDR,
                             T_OWNKEY: C_ADDR_UNO},
             MD_SPEC_TABLE: {T_KEY: C_CPSO_NO},
             MD_REG_JURISDIC: {T_KEY: C_CPSO_NO, T_VAL: C_JUR_CODE}
             }

TEST_CPSO = (10310, 102166,
             102165, 84993, 85144, 85179, 85950, 87827, 87890, 90572,
             100175, 70129, 110259, 110260, 70130, 108493,
             101169, 101170, 101079, 101080, 100326,
             150001, 150002, 150003, 150004,
             100248, 70079, 100249, 100174, 100023, 10022,
             94129, 115098, 10065, 22278, 110310, 82099, 86495)

CPSO_START = 10000
CPSO_STOP = 140100

API_KEY = 'AIzaSyBkKoxxJxWNpPluVYD0HRt3ya05HctSTn4'

GEO_TABLE = 'MD_geo_pos'
ADDR_X_GEO_TABLE = 'MD_addr_x_geo'

FINAL_SQL = ["UPDATE MD_addresses "
             "SET phone_no_clean = "
             r"regexp_replace(phone_no, '\\D', ''), "
             r"fax_no_clean = regexp_replace(fax_no, '\\D', '') "
             f"WHERE cpso_no = ?",

             "UPDATE MD_addresses "
             "SET fax_no_email = CONCAT(fax_no_clean, "
             "'@rcfaxbroadcast.com') "
             "WHERE COALESCE(fax_no_clean, '') <> '' "
             "AND cpso_no = ?",

             "UPDATE MD_addresses "
             "SET postal_code = upper(regexp_replace(postal_code, "
             "'[^A-z0-9]', '')) "
             "WHERE postal_code REGEXP "
             "'[A-z][0-9][A-z][^A-z0-9]+[0-9][A-z][0-9]' "
             "AND cpso_no = ?",

             "UPDATE MD_addresses "
             "SET postal_code_clean = postal_code "
             "WHERE postal_code "
             "REGEXP '[A-Z][0-9][A-Z][0-9][A-Z][0-9]' "
             "AND cpso_no = ?",

             "UPDATE MD_addresses "
             "SET postal_code = insert(postal_code_clean, 4, 0, ' ') "
             "WHERE postal_code_clean "
             "REGEXP '[A-Z][0-9][A-Z][0-9][A-Z][0-9]' "
             "AND cpso_no = ?",

             "UPDATE z847e_MD_dir d "
             "JOIN MD_addresses adr ON adr.isDefault "
             "AND d.CPSO_no = adr.CPSO_no AND adr.isActive "
             "SET d.address_1 = adr.address_1, "
             "d.address_2 = adr.address_2, "
             "d.address_3 = adr.address_3, "
             "d.address_4 = adr.address_4, "
             "d.city = adr.city, d.prov_code = adr.prov_code, "
             "d.postal_code = adr.postal_code, "
             "d.country = adr.country, "
             "d.addresses_raw = adr.address_raw, "
             "d.phone_no = adr.phone_no, d.ext_no = adr.ext_no, "
             "d.phone_no_clean = adr.phone_no_clean, "
             "d.fax_no = adr.fax_no, "
             "d.fax_no_clean = adr.fax_no_clean, "
             "d.fax_no_email = adr.fax_no_email, "
             "d.last_modified = NOW(), "
             "d.district_no = adr.district_no "
             "WHERE d.cpso_no = ?"
             ]
