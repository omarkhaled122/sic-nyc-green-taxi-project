# NYC Taxi Zone Coordinates Mapping
# Each zone has street-level coordinates avoiding water/parks/restricted areas
import logging

nyc_taxi_zone_coordinates = {
    # Newark Airport
    1: {"lat_range": (40.6895, 40.6925), "lon_range": (-74.1745, -74.1685)},  # EWR terminals
    
    # QUEENS ZONES
    2: {"lat_range": (40.6025, 40.6065), "lon_range": (-73.8365, -73.8325)},  # Jamaica Bay - Cross Bay Blvd area
    7: {"lat_range": (40.7592, 40.7792), "lon_range": (-73.9302, -73.9102)},  # Astoria
    8: {"lat_range": (40.7751, 40.7801), "lon_range": (-73.9251, -73.9201)},  # Astoria Park area
    9: {"lat_range": (40.7531, 40.7581), "lon_range": (-73.7931, -73.7881)},  # Auburndale
    10: {"lat_range": (40.6760, 40.6810), "lon_range": (-73.7860, -73.7810)},  # Baisley Park
    15: {"lat_range": (40.8023, 40.8073), "lon_range": (-73.8023, -73.7973)},  # Bay Terrace/Fort Totten
    16: {"lat_range": (40.7614, 40.7714), "lon_range": (-73.7814, -73.7714)},  # Bayside
    19: {"lat_range": (40.7234, 40.7284), "lon_range": (-73.7434, -73.7384)},  # Bellerose
    27: {"lat_range": (40.5590, 40.5640), "lon_range": (-73.9290, -73.9240)},  # Breezy Point - accessible roads only
    28: {"lat_range": (40.7090, 40.7140), "lon_range": (-73.8290, -73.8240)},  # Briarwood/Jamaica Hills
    30: {"lat_range": (40.6031, 40.6081), "lon_range": (-73.8231, -73.8181)},  # Broad Channel
    38: {"lat_range": (40.6912, 40.6962), "lon_range": (-73.7412, -73.7362)},  # Cambria Heights
    53: {"lat_range": (40.7854, 40.7904), "lon_range": (-73.8454, -73.8404)},  # College Point
    56: {"lat_range": (40.7469, 40.7519), "lon_range": (-73.8669, -73.8619)},  # Corona (first instance)
    57: {"lat_range": (40.7394, 40.7444), "lon_range": (-73.8594, -73.8544)},  # Corona (second instance)
    64: {"lat_range": (40.7747, 40.7797), "lon_range": (-73.7547, -73.7497)},  # Douglaston
    70: {"lat_range": (40.7615, 40.7665), "lon_range": (-73.8815, -73.8765)},  # East Elmhurst
    73: {"lat_range": (40.7389, 40.7439), "lon_range": (-73.7889, -73.7839)},  # East Flushing
    82: {"lat_range": (40.7352, 40.7452), "lon_range": (-73.8852, -73.8752)},  # Elmhurst
    83: {"lat_range": (40.7298, 40.7348), "lon_range": (-73.8998, -73.8948)},  # Elmhurst/Maspeth
    86: {"lat_range": (40.6050, 40.6150), "lon_range": (-73.7550, -73.7450)},  # Far Rockaway
    92: {"lat_range": (40.7580, 40.7680), "lon_range": (-73.8380, -73.8280)},  # Flushing
    93: {"lat_range": (40.7467, 40.7517), "lon_range": (-73.8467, -73.8417)},  # Flushing Meadows - perimeter roads
    95: {"lat_range": (40.7184, 40.7284), "lon_range": (-73.8484, -73.8384)},  # Forest Hills
    96: {"lat_range": (40.7009, 40.7059), "lon_range": (-73.8509, -73.8459)},  # Forest Park - perimeter
    98: {"lat_range": (40.7347, 40.7397), "lon_range": (-73.7847, -73.7797)},  # Fresh Meadows
    101: {"lat_range": (40.7481, 40.7531), "lon_range": (-73.7681, -73.7631)},  # Glen Oaks
    102: {"lat_range": (40.7016, 40.7066), "lon_range": (-73.8716, -73.8666)},  # Glendale
    117: {"lat_range": (40.5841, 40.5891), "lon_range": (-73.8241, -73.8191)},  # Hammels/Arverne
    121: {"lat_range": (40.7127, 40.7177), "lon_range": (-73.8127, -73.8077)},  # Hillcrest/Pomonok
    122: {"lat_range": (40.7089, 40.7139), "lon_range": (-73.7689, -73.7639)},  # Hollis
    124: {"lat_range": (40.6589, 40.6689), "lon_range": (-73.8389, -73.8289)},  # Howard Beach
    129: {"lat_range": (40.7489, 40.7589), "lon_range": (-73.8889, -73.8789)},  # Jackson Heights
    130: {"lat_range": (40.7028, 40.7128), "lon_range": (-73.8028, -73.7928)},  # Jamaica
    131: {"lat_range": (40.7147, 40.7247), "lon_range": (-73.7847, -73.7747)},  # Jamaica Estates
    132: {"lat_range": (40.6413, 40.6463), "lon_range": (-73.7913, -73.7863)},  # JFK Airport
    134: {"lat_range": (40.7099, 40.7149), "lon_range": (-73.8349, -73.8299)},  # Kew Gardens
    135: {"lat_range": (40.7209, 40.7259), "lon_range": (-73.8259, -73.8209)},  # Kew Gardens Hills
    138: {"lat_range": (40.7731, 40.7781), "lon_range": (-73.8731, -73.8681)},  # LaGuardia Airport
    139: {"lat_range": (40.6741, 40.6791), "lon_range": (-73.7541, -73.7491)},  # Laurelton
    145: {"lat_range": (40.7447, 40.7547), "lon_range": (-73.9547, -73.9447)},  # LIC/Hunters Point
    146: {"lat_range": (40.7507, 40.7557), "lon_range": (-73.9407, -73.9357)},  # LIC/Queens Plaza
    157: {"lat_range": (40.7243, 40.7293), "lon_range": (-73.9093, -73.9043)},  # Maspeth
    160: {"lat_range": (40.7133, 40.7183), "lon_range": (-73.8833, -73.8783)},  # Middle Village
    171: {"lat_range": (40.7641, 40.7691), "lon_range": (-73.8141, -73.8091)},  # Murray Hill-Queens
    173: {"lat_range": (40.7594, 40.7644), "lon_range": (-73.8744, -73.8694)},  # North Corona
    175: {"lat_range": (40.7372, 40.7422), "lon_range": (-73.7572, -73.7522)},  # Oakland Gardens
    179: {"lat_range": (40.7702, 40.7752), "lon_range": (-73.9102, -73.9052)},  # Old Astoria
    180: {"lat_range": (40.6816, 40.6916), "lon_range": (-73.8516, -73.8416)},  # Ozone Park
    191: {"lat_range": (40.7147, 40.7247), "lon_range": (-73.7447, -73.7347)},  # Queens Village
    192: {"lat_range": (40.7482, 40.7532), "lon_range": (-73.8182, -73.8132)},  # Queensboro Hill
    193: {"lat_range": (40.7563, 40.7613), "lon_range": (-73.9463, -73.9413)},  # Queensbridge/Ravenswood
    196: {"lat_range": (40.7264, 40.7364), "lon_range": (-73.8664, -73.8564)},  # Rego Park
    197: {"lat_range": (40.6995, 40.7095), "lon_range": (-73.8395, -73.8295)},  # Richmond Hill
    198: {"lat_range": (40.7058, 40.7158), "lon_range": (-73.9058, -73.8958)},  # Ridgewood
    201: {"lat_range": (40.5803, 40.5853), "lon_range": (-73.8403, -73.8353)},  # Rockaway Park
    203: {"lat_range": (40.6579, 40.6629), "lon_range": (-73.7479, -73.7429)},  # Rosedale
    205: {"lat_range": (40.6845, 40.6945), "lon_range": (-73.7645, -73.7545)},  # Saint Albans
    207: {"lat_range": (40.7397, 40.7447), "lon_range": (-73.9147, -73.9097)},  # St Michaels Cemetery/Woodside
    215: {"lat_range": (40.6666, 40.6766), "lon_range": (-73.7966, -73.7866)},  # South Jamaica
    216: {"lat_range": (40.6741, 40.6841), "lon_range": (-73.8141, -73.8041)},  # South Ozone Park
    218: {"lat_range": (40.6625, 40.6675), "lon_range": (-73.7725, -73.7675)},  # Springfield Gardens North
    219: {"lat_range": (40.6505, 40.6555), "lon_range": (-73.7605, -73.7555)},  # Springfield Gardens South
    223: {"lat_range": (40.7743, 40.7793), "lon_range": (-73.9043, -73.8993)},  # Steinway
    226: {"lat_range": (40.7432, 40.7482), "lon_range": (-73.9232, -73.9182)},  # Sunnyside
    252: {"lat_range": (40.7947, 40.7997), "lon_range": (-73.8147, -73.8097)},  # Whitestone
    253: {"lat_range": (40.7578, 40.7628), "lon_range": (-73.8478, -73.8428)},  # Willets Point
    258: {"lat_range": (40.6927, 40.7027), "lon_range": (-73.8727, -73.8627)},  # Woodhaven
    260: {"lat_range": (40.7394, 40.7494), "lon_range": (-73.9094, -73.8994)},  # Woodside
    
    # BROOKLYN ZONES
    11: {"lat_range": (40.6016, 40.6116), "lon_range": (-74.0016, -73.9916)},  # Bath Beach
    14: {"lat_range": (40.6295, 40.6445), "lon_range": (-74.0395, -74.0245)},  # Bay Ridge
    17: {"lat_range": (40.6870, 40.6970), "lon_range": (-73.9570, -73.9470)},  # Bedford
    21: {"lat_range": (40.5946, 40.6046), "lon_range": (-73.9896, -73.9796)},  # Bensonhurst East
    22: {"lat_range": (40.6047, 40.6147), "lon_range": (-74.0047, -73.9947)},  # Bensonhurst West
    25: {"lat_range": (40.6863, 40.6913), "lon_range": (-73.9913, -73.9863)},  # Boerum Hill
    26: {"lat_range": (40.6339, 40.6439), "lon_range": (-73.9989, -73.9889)},  # Borough Park
    29: {"lat_range": (40.5778, 40.5828), "lon_range": (-73.9678, -73.9628)},  # Brighton Beach
    33: {"lat_range": (40.6960, 40.7010), "lon_range": (-73.9960, -73.9910)},  # Brooklyn Heights
    34: {"lat_range": (40.7029, 40.7079), "lon_range": (-73.9729, -73.9679)},  # Brooklyn Navy Yard
    35: {"lat_range": (40.6651, 40.6751), "lon_range": (-73.9201, -73.9101)},  # Brownsville
    36: {"lat_range": (40.6961, 40.7061), "lon_range": (-73.9411, -73.9311)},  # Bushwick North
    37: {"lat_range": (40.6861, 40.6961), "lon_range": (-73.9261, -73.9161)},  # Bushwick South
    39: {"lat_range": (40.6439, 40.6539), "lon_range": (-73.8939, -73.8839)},  # Canarsie
    40: {"lat_range": (40.6794, 40.6844), "lon_range": (-73.9994, -73.9944)},  # Carroll Gardens
    49: {"lat_range": (40.6903, 40.6953), "lon_range": (-73.9703, -73.9653)},  # Clinton Hill
    52: {"lat_range": (40.6860, 40.6910), "lon_range": (-74.0010, -73.9960)},  # Cobble Hill
    54: {"lat_range": (40.6792, 40.6842), "lon_range": (-74.0092, -74.0042)},  # Columbia Street
    55: {"lat_range": (40.5731, 40.5781), "lon_range": (-73.9831, -73.9781)},  # Coney Island
    61: {"lat_range": (40.6694, 40.6794), "lon_range": (-73.9594, -73.9494)},  # Crown Heights North
    62: {"lat_range": (40.6594, 40.6694), "lon_range": (-73.9494, -73.9394)},  # Crown Heights South
    63: {"lat_range": (40.6893, 40.6993), "lon_range": (-73.8843, -73.8743)},  # Cypress Hills
    65: {"lat_range": (40.6928, 40.6978), "lon_range": (-73.9878, -73.9828)},  # Downtown Brooklyn
    66: {"lat_range": (40.7033, 40.7083), "lon_range": (-73.9883, -73.9833)},  # DUMBO/Vinegar Hill
    67: {"lat_range": (40.6212, 40.6312), "lon_range": (-74.0162, -74.0062)},  # Dyker Heights
    71: {"lat_range": (40.6496, 40.6596), "lon_range": (-73.9596, -73.9496)},  # East Flatbush/Farragut
    72: {"lat_range": (40.6396, 40.6496), "lon_range": (-73.9496, -73.9396)},  # East Flatbush/Remsen Village
    76: {"lat_range": (40.6566, 40.6666), "lon_range": (-73.8916, -73.8816)},  # East New York
    77: {"lat_range": (40.6666, 40.6766), "lon_range": (-73.8966, -73.8866)},  # East New York/Pennsylvania Ave
    80: {"lat_range": (40.7102, 40.7152), "lon_range": (-73.9402, -73.9352)},  # East Williamsburg
    85: {"lat_range": (40.6494, 40.6594), "lon_range": (-73.9394, -73.9294)},  # Erasmus
    89: {"lat_range": (40.6395, 40.6495), "lon_range": (-73.9695, -73.9595)},  # Flatbush/Ditmas Park
    91: {"lat_range": (40.6196, 40.6296), "lon_range": (-73.9346, -73.9246)},  # Flatlands
    97: {"lat_range": (40.6881, 40.6931), "lon_range": (-73.9781, -73.9731)},  # Fort Greene
    106: {"lat_range": (40.6732, 40.6782), "lon_range": (-73.9882, -73.9832)},  # Gowanus
    108: {"lat_range": (40.5989, 40.6089), "lon_range": (-73.9789, -73.9689)},  # Gravesend
    111: {"lat_range": (40.6548, 40.6598), "lon_range": (-73.9948, -73.9898)},  # Green-Wood Cemetery - perimeter
    112: {"lat_range": (40.7294, 40.7394), "lon_range": (-73.9594, -73.9494)},  # Greenpoint
    123: {"lat_range": (40.5963, 40.6063), "lon_range": (-73.9713, -73.9613)},  # Homecrest
    133: {"lat_range": (40.6347, 40.6447), "lon_range": (-73.9797, -73.9697)},  # Kensington
    149: {"lat_range": (40.5797, 40.5897), "lon_range": (-73.9647, -73.9547)},  # Madison
    150: {"lat_range": (40.5786, 40.5836), "lon_range": (-73.9486, -73.9436)},  # Manhattan Beach
    154: {"lat_range": (40.6048, 40.6098), "lon_range": (-73.9298, -73.9248)},  # Marine Park/Floyd Bennett - roads only
    155: {"lat_range": (40.6148, 40.6248), "lon_range": (-73.9148, -73.9048)},  # Marine Park/Mill Basin
    165: {"lat_range": (40.6247, 40.6347), "lon_range": (-73.9647, -73.9547)},  # Midwood
    177: {"lat_range": (40.6700, 40.6800), "lon_range": (-73.9150, -73.9050)},  # Ocean Hill
    178: {"lat_range": (40.6097, 40.6197), "lon_range": (-73.9747, -73.9647)},  # Ocean Parkway South
    181: {"lat_range": (40.6748, 40.6848), "lon_range": (-73.9798, -73.9698)},  # Park Slope
    188: {"lat_range": (40.6548, 40.6648), "lon_range": (-73.9698, -73.9598)},  # Prospect-Lefferts Gardens
    189: {"lat_range": (40.6773, 40.6823), "lon_range": (-73.9673, -73.9623)},  # Prospect Heights
    190: {"lat_range": (40.6616, 40.6666), "lon_range": (-73.9716, -73.9666)},  # Prospect Park - perimeter streets
    195: {"lat_range": (40.6754, 40.6804), "lon_range": (-74.0104, -74.0054)},  # Red Hook
    210: {"lat_range": (40.6162, 40.6262), "lon_range": (-73.9612, -73.9512)},  # Sheepshead Bay
    217: {"lat_range": (40.7089, 40.7189), "lon_range": (-73.9589, -73.9489)},  # South Williamsburg
    222: {"lat_range": (40.6470, 40.6520), "lon_range": (-73.8820, -73.8770)},  # Starrett City
    225: {"lat_range": (40.6746, 40.6846), "lon_range": (-73.9296, -73.9196)},  # Stuyvesant Heights
    227: {"lat_range": (40.6495, 40.6595), "lon_range": (-73.9995, -73.9895)},  # Sunset Park East
    228: {"lat_range": (40.6545, 40.6645), "lon_range": (-74.0095, -73.9995)},  # Sunset Park West
    255: {"lat_range": (40.7138, 40.7238), "lon_range": (-73.9588, -73.9488)},  # Williamsburg (North Side)
    256: {"lat_range": (40.7038, 40.7138), "lon_range": (-73.9638, -73.9538)},  # Williamsburg (South Side)
    257: {"lat_range": (40.6597, 40.6647), "lon_range": (-73.9847, -73.9797)},  # Windsor Terrace
    
    # BRONX ZONES
    3: {"lat_range": (40.8650, 40.8750), "lon_range": (-73.8500, -73.8400)},  # Allerton/Pelham Gardens
    18: {"lat_range": (40.8686, 40.8786), "lon_range": (-73.8986, -73.8886)},  # Bedford Park
    20: {"lat_range": (40.8510, 40.8610), "lon_range": (-73.8910, -73.8810)},  # Belmont
    31: {"lat_range": (40.8553, 40.8603), "lon_range": (-73.8803, -73.8753)},  # Bronx Park - perimeter
    32: {"lat_range": (40.8481, 40.8581), "lon_range": (-73.8631, -73.8531)},  # Bronxdale
    46: {"lat_range": (40.8434, 40.8484), "lon_range": (-73.7834, -73.7784)},  # City Island
    47: {"lat_range": (40.8398, 40.8498), "lon_range": (-73.9098, -73.8998)},  # Claremont/Bathgate
    51: {"lat_range": (40.8748, 40.8848), "lon_range": (-73.8348, -73.8248)},  # Co-Op City
    58: {"lat_range": (40.8324, 40.8424), "lon_range": (-73.8274, -73.8174)},  # Country Club
    59: {"lat_range": (40.8393, 40.8443), "lon_range": (-73.8943, -73.8893)},  # Crotona Park - perimeter
    60: {"lat_range": (40.8393, 40.8493), "lon_range": (-73.8843, -73.8743)},  # Crotona Park East
    69: {"lat_range": (40.8138, 40.8238), "lon_range": (-73.9288, -73.9188)},  # East Concourse/Concourse Village
    74: {"lat_range": (40.7947, 40.8047), "lon_range": (-73.9447, -73.9347)},  # East Harlem North (Manhattan)
    75: {"lat_range": (40.7847, 40.7947), "lon_range": (-73.9497, -73.9397)},  # East Harlem South (Manhattan)
    78: {"lat_range": (40.8453, 40.8553), "lon_range": (-73.8953, -73.8853)},  # East Tremont
    81: {"lat_range": (40.8851, 40.8951), "lon_range": (-73.8301, -73.8201)},  # Eastchester
    94: {"lat_range": (40.8487, 40.8587), "lon_range": (-73.8987, -73.8887)},  # Fordham South
    119: {"lat_range": (40.8437, 40.8537), "lon_range": (-73.9287, -73.9187)},  # Highbridge
    126: {"lat_range": (40.8088, 40.8188), "lon_range": (-73.8838, -73.8738)},  # Hunts Point
    136: {"lat_range": (40.8638, 40.8738), "lon_range": (-73.9188, -73.9088)},  # Kingsbridge Heights
    147: {"lat_range": (40.8243, 40.8343), "lon_range": (-73.8893, -73.8793)},  # Longwood
    159: {"lat_range": (40.8193, 40.8293), "lon_range": (-73.9143, -73.9043)},  # Melrose South
    167: {"lat_range": (40.8243, 40.8343), "lon_range": (-73.9043, -73.8943)},  # Morrisania/Melrose
    168: {"lat_range": (40.8043, 40.8143), "lon_range": (-73.9243, -73.9143)},  # Mott Haven/Port Morris
    169: {"lat_range": (40.8293, 40.8393), "lon_range": (-73.9093, -73.8993)},  # Mount Hope
    174: {"lat_range": (40.8788, 40.8888), "lon_range": (-73.8888, -73.8788)},  # Norwood
    182: {"lat_range": (40.8333, 40.8433), "lon_range": (-73.8533, -73.8433)},  # Parkchester
    183: {"lat_range": (40.8654, 40.8754), "lon_range": (-73.8254, -73.8154)},  # Pelham Bay
    184: {"lat_range": (40.8554, 40.8604), "lon_range": (-73.8104, -73.8054)},  # Pelham Bay Park - perimeter
    185: {"lat_range": (40.8574, 40.8674), "lon_range": (-73.8674, -73.8574)},  # Pelham Parkway
    199: {"lat_range": (40.8018, 40.8068), "lon_range": (-73.8768, -73.8718)},  # Rikers Island - restricted
    200: {"lat_range": (40.8988, 40.9088), "lon_range": (-73.9138, -73.9038)},  # Riverdale/North Riverdale
    208: {"lat_range": (40.8454, 40.8554), "lon_range": (-73.8154, -73.8054)},  # Schuylerville/Edgewater Park
    212: {"lat_range": (40.8173, 40.8273), "lon_range": (-73.8523, -73.8423)},  # Soundview/Bruckner
    213: {"lat_range": (40.8223, 40.8323), "lon_range": (-73.8473, -73.8373)},  # Soundview/Castle Hill
    220: {"lat_range": (40.8779, 40.8879), "lon_range": (-73.9229, -73.9129)},  # Spuyten Duyvil/Kingsbridge
    235: {"lat_range": (40.8488, 40.8588), "lon_range": (-73.9238, -73.9138)},  # University Heights
    240: {"lat_range": (40.8903, 40.8953), "lon_range": (-73.8803, -73.8753)},  # Van Cortlandt Park - perimeter
    241: {"lat_range": (40.8853, 40.8953), "lon_range": (-73.8903, -73.8803)},  # Van Cortlandt Village
    242: {"lat_range": (40.8454, 40.8554), "lon_range": (-73.8504, -73.8404)},  # Van Nest/Morris Park
    247: {"lat_range": (40.8338, 40.8438), "lon_range": (-73.9338, -73.9238)},  # West Concourse
    248: {"lat_range": (40.8188, 40.8288), "lon_range": (-73.8788, -73.8688)},  # West Farms/Bronx River
    250: {"lat_range": (40.8291, 40.8391), "lon_range": (-73.8591, -73.8491)},  # Westchester Village/Unionport
    254: {"lat_range": (40.8754, 40.8854), "lon_range": (-73.8654, -73.8554)},  # Williamsbridge/Olinville
    259: {"lat_range": (40.8954, 40.9054), "lon_range": (-73.8604, -73.8504)},  # Woodlawn/Wakefield
    
    # MANHATTAN ZONES
    4: {"lat_range": (40.7183, 40.7283), "lon_range": (-73.9833, -73.9733)},  # Alphabet City
    12: {"lat_range": (40.7014, 40.7064), "lon_range": (-74.0164, -74.0114)},  # Battery Park
    13: {"lat_range": (40.7074, 40.7124), "lon_range": (-74.0174, -74.0124)},  # Battery Park City
    24: {"lat_range": (40.7764, 40.7864), "lon_range": (-73.9864, -73.9764)},  # Bloomingdale
    41: {"lat_range": (40.8089, 40.8189), "lon_range": (-73.9539, -73.9439)},  # Central Harlem
    42: {"lat_range": (40.8189, 40.8289), "lon_range": (-73.9489, -73.9389)},  # Central Harlem North
    43: {"lat_range": (40.7829, 40.7879), "lon_range": (-73.9679, -73.9629)},  # Central Park - perimeter streets
    45: {"lat_range": (40.7130, 40.7180), "lon_range": (-74.0030, -73.9980)},  # Chinatown
    48: {"lat_range": (40.7636, 40.7736), "lon_range": (-73.9936, -73.9836)},  # Clinton East
    50: {"lat_range": (40.7586, 40.7686), "lon_range": (-74.0036, -73.9936)},  # Clinton West
    68: {"lat_range": (40.7437, 40.7537), "lon_range": (-73.9937, -73.9837)},  # East Chelsea
    79: {"lat_range": (40.7234, 40.7334), "lon_range": (-73.9884, -73.9784)},  # East Village
    87: {"lat_range": (40.7089, 40.7139), "lon_range": (-74.0139, -74.0089)},  # Financial District North
    88: {"lat_range": (40.7039, 40.7089), "lon_range": (-74.0189, -74.0139)},  # Financial District South
    90: {"lat_range": (40.7398, 40.7448), "lon_range": (-73.9948, -73.9898)},  # Flatiron
    100: {"lat_range": (40.7547, 40.7647), "lon_range": (-73.9897, -73.9797)},  # Garment District
    103: {"lat_range": (40.6892, 40.6942), "lon_range": (-74.0192, -74.0142)},  # Governor's Island
    104: {"lat_range": (40.6992, 40.7042), "lon_range": (-74.0442, -74.0392)},  # Ellis Island
    105: {"lat_range": (40.6895, 40.6945), "lon_range": (-74.0495, -74.0445)},  # Liberty Island
    107: {"lat_range": (40.7378, 40.7428), "lon_range": (-73.9878, -73.9828)},  # Gramercy
    113: {"lat_range": (40.7324, 40.7374), "lon_range": (-74.0074, -74.0024)},  # Greenwich Village North
    114: {"lat_range": (40.7274, 40.7324), "lon_range": (-74.0074, -74.0024)},  # Greenwich Village South
    116: {"lat_range": (40.8214, 40.8314), "lon_range": (-73.9564, -73.9464)},  # Hamilton Heights
    120: {"lat_range": (40.8389, 40.8439), "lon_range": (-73.9439, -73.9389)},  # Highbridge Park - perimeter
    125: {"lat_range": (40.7274, 40.7324), "lon_range": (-74.0074, -74.0024)},  # Hudson Sq
    127: {"lat_range": (40.8664, 40.8764), "lon_range": (-73.9264, -73.9164)},  # Inwood
    128: {"lat_range": (40.8714, 40.8764), "lon_range": (-73.9214, -73.9164)},  # Inwood Hill Park - perimeter
    137: {"lat_range": (40.7478, 40.7578), "lon_range": (-73.9778, -73.9678)},  # Kips Bay
    140: {"lat_range": (40.7636, 40.7736), "lon_range": (-73.9636, -73.9536)},  # Lenox Hill East
    141: {"lat_range": (40.7636, 40.7736), "lon_range": (-73.9736, -73.9636)},  # Lenox Hill West
    142: {"lat_range": (40.7688, 40.7788), "lon_range": (-73.9838, -73.9738)},  # Lincoln Square East
    143: {"lat_range": (40.7688, 40.7788), "lon_range": (-73.9888, -73.9788)},  # Lincoln Square West
    144: {"lat_range": (40.7194, 40.7244), "lon_range": (-73.9994, -73.9944)},  # Little Italy/NoLiTa
    148: {"lat_range": (40.7142, 40.7242), "lon_range": (-73.9892, -73.9792)},  # Lower East Side
    151: {"lat_range": (40.7989, 40.8089), "lon_range": (-73.9689, -73.9589)},  # Manhattan Valley
    152: {"lat_range": (40.8139, 40.8239), "lon_range": (-73.9589, -73.9489)},  # Manhattanville
    153: {"lat_range": (40.8764, 40.8814), "lon_range": (-73.9114, -73.9064)},  # Marble Hill
    158: {"lat_range": (40.7374, 40.7424), "lon_range": (-74.0124, -74.0074)},  # Meatpacking/West Village West
    161: {"lat_range": (40.7547, 40.7647), "lon_range": (-73.9847, -73.9747)},  # Midtown Center
    162: {"lat_range": (40.7528, 40.7628), "lon_range": (-73.9728, -73.9628)},  # Midtown East
    163: {"lat_range": (40.7587, 40.7687), "lon_range": (-73.9837, -73.9737)},  # Midtown North
    164: {"lat_range": (40.7487, 40.7587), "lon_range": (-73.9887, -73.9787)},  # Midtown South
    166: {"lat_range": (40.8089, 40.8189), "lon_range": (-73.9639, -73.9539)},  # Morningside Heights
    170: {"lat_range": (40.7478, 40.7578), "lon_range": (-73.9828, -73.9728)},  # Murray Hill
    186: {"lat_range": (40.7497, 40.7547), "lon_range": (-73.9947, -73.9897)},  # Penn Station/Madison Sq West
    194: {"lat_range": (40.7689, 40.7739), "lon_range": (-73.9289, -73.9239)},  # Randalls Island
    202: {"lat_range": (40.7549, 40.7649), "lon_range": (-73.9549, -73.9449)},  # Roosevelt Island
    209: {"lat_range": (40.7089, 40.7139), "lon_range": (-74.0089, -74.0039)},  # Seaport
    211: {"lat_range": (40.7224, 40.7274), "lon_range": (-74.0024, -73.9974)},  # SoHo
    224: {"lat_range": (40.7317, 40.7367), "lon_range": (-73.9817, -73.9767)},  # Stuy Town/Peter Cooper Village
    229: {"lat_range": (40.7528, 40.7578), "lon_range": (-73.9678, -73.9628)},  # Sutton Place/Turtle Bay North
    230: {"lat_range": (40.7560, 40.7610), "lon_range": (-73.9860, -73.9810)},  # Times Sq/Theatre District
    231: {"lat_range": (40.7163, 40.7213), "lon_range": (-74.0113, -74.0063)},  # TriBeCa/Civic Center
    232: {"lat_range": (40.7114, 40.7164), "lon_range": (-73.9914, -73.9864)},  # Two Bridges/Seward Park
    233: {"lat_range": (40.7478, 40.7528), "lon_range": (-73.9728, -73.9678)},  # UN/Turtle Bay South
    234: {"lat_range": (40.7347, 40.7397), "lon_range": (-73.9897, -73.9847)},  # Union Sq
    236: {"lat_range": (40.7736, 40.7836), "lon_range": (-73.9586, -73.9486)},  # Upper East Side North
    237: {"lat_range": (40.7636, 40.7736), "lon_range": (-73.9636, -73.9536)},  # Upper East Side South
    238: {"lat_range": (40.7936, 40.8036), "lon_range": (-73.9736, -73.9636)},  # Upper West Side North
    239: {"lat_range": (40.7736, 40.7836), "lon_range": (-73.9786, -73.9686)},  # Upper West Side South
    243: {"lat_range": (40.8514, 40.8614), "lon_range": (-73.9464, -73.9364)},  # Washington Heights North
    244: {"lat_range": (40.8314, 40.8414), "lon_range": (-73.9514, -73.9414)},  # Washington Heights South
    246: {"lat_range": (40.7487, 40.7587), "lon_range": (-74.0087, -73.9987)},  # West Chelsea/Hudson Yards
    249: {"lat_range": (40.7324, 40.7374), "lon_range": (-74.0074, -74.0024)},  # West Village
    261: {"lat_range": (40.7089, 40.7139), "lon_range": (-74.0139, -74.0089)},  # World Trade Center
    262: {"lat_range": (40.7736, 40.7836), "lon_range": (-73.9536, -73.9436)},  # Yorkville East
    263: {"lat_range": (40.7736, 40.7836), "lon_range": (-73.9636, -73.9536)},  # Yorkville West
    
    # STATEN ISLAND ZONES
    5: {"lat_range": (40.5564, 40.5614), "lon_range": (-74.1914, -74.1864)},  # Arden Heights
    6: {"lat_range": (40.5964, 40.6014), "lon_range": (-74.1064, -74.1014)},  # Arrochar/Fort Wadsworth
    23: {"lat_range": (40.5814, 40.5864), "lon_range": (-74.1564, -74.1514)},  # Bloomfield/Emerson Hill
    44: {"lat_range": (40.5089, 40.5139), "lon_range": (-74.2439, -74.2389)},  # Charleston/Tottenville
    84: {"lat_range": (40.5464, 40.5514), "lon_range": (-74.2014, -74.1964)},  # Eltingville/Annadale
    99: {"lat_range": (40.5814, 40.5864), "lon_range": (-74.1914, -74.1864)},  # Freshkills Park - perimeter
    109: {"lat_range": (40.5664, 40.5714), "lon_range": (-74.1564, -74.1514)},  # Great Kills
    110: {"lat_range": (40.5564, 40.5614), "lon_range": (-74.1464, -74.1414)},  # Great Kills Park - perimeter
    115: {"lat_range": (40.6014, 40.6064), "lon_range": (-74.0814, -74.0764)},  # Grymes Hill/Clifton
    118: {"lat_range": (40.5888, 40.5938), "lon_range": (-74.1688, -74.1638)},  # Heartland Village/Todt Hill
    156: {"lat_range": (40.6414, 40.6464), "lon_range": (-74.1664, -74.1614)},  # Mariners Harbor
    172: {"lat_range": (40.5764, 40.5814), "lon_range": (-74.1264, -74.1214)},  # New Dorp/Midland Beach
    176: {"lat_range": (40.5614, 40.5664), "lon_range": (-74.1364, -74.1314)},  # Oakwood
    187: {"lat_range": (40.6364, 40.6414), "lon_range": (-74.1514, -74.1464)},  # Port Richmond
    204: {"lat_range": (40.5239, 40.5289), "lon_range": (-74.2139, -74.2089)},  # Rossville/Woodrow
    206: {"lat_range": (40.6424, 40.6474), "lon_range": (-74.0824, -74.0774)},  # Saint George/New Brighton
    214: {"lat_range": (40.5814, 40.5864), "lon_range": (-74.0864, -74.0814)},  # South Beach/Dongan Hills
    221: {"lat_range": (40.6264, 40.6314), "lon_range": (-74.0814, -74.0764)},  # Stapleton
    245: {"lat_range": (40.6214, 40.6264), "lon_range": (-74.1164, -74.1114)},  # West Brighton
    251: {"lat_range": (40.6139, 40.6189), "lon_range": (-74.1439, -74.1389)},  # Westerleigh
    
    # SPECIAL ZONES
    264: {"lat_range": (40.7500, 40.7600), "lon_range": (-73.9800, -73.9700)},  # Unknown - default to Midtown
    265: {"lat_range": None, "lon_range": None}  # N/A - Outside NYC
}
import pandas as pd
import numpy as np
import math

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    Returns distance in miles
    """
    # Radius of earth in miles
    R = 3959.0
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def generate_destination_coordinates(start_lat, start_lon, distance_miles, dest_zone_info):
    """
    Generate destination coordinates that are approximately 'distance_miles' away
    from the start coordinates, but still within the destination zone boundaries
    """
    if not dest_zone_info["lat_range"] or not dest_zone_info["lon_range"]:
        return None, None
    
    # Convert miles to degrees (rough approximation)
    # 1 degree latitude ≈ 69 miles
    # 1 degree longitude ≈ 55 miles at NYC latitude
    lat_per_mile = 1 / 69.0
    lon_per_mile = 1 / 55.0
    
    # Try multiple times to find valid coordinates
    for attempt in range(50):
        # Generate random angle for direction
        angle = np.random.uniform(0, 2 * np.pi)
        
        # Calculate approximate offset
        lat_offset = distance_miles * lat_per_mile * np.sin(angle)
        lon_offset = distance_miles * lon_per_mile * np.cos(angle)
        
        # Calculate new coordinates
        new_lat = start_lat + lat_offset
        new_lon = start_lon + lon_offset
        
        # Check if coordinates are within destination zone boundaries
        if (dest_zone_info["lat_range"][0] <= new_lat <= dest_zone_info["lat_range"][1] and
            dest_zone_info["lon_range"][0] <= new_lon <= dest_zone_info["lon_range"][1]):
            
            # Verify actual distance is close to desired distance
            actual_distance = haversine_distance(start_lat, start_lon, new_lat, new_lon)
            if abs(actual_distance - distance_miles) / distance_miles < 0.3:  # Within 30% of target
                return round(new_lat, 6), round(new_lon, 6)
    
    # If we couldn't find coordinates at exact distance, just use random coords in zone
    # but prefer edges of zone to maximize distance
    if distance_miles > 0.5:  # For longer trips
        # Try to use opposite corners
        corners = [
            (dest_zone_info["lat_range"][0], dest_zone_info["lon_range"][0]),
            (dest_zone_info["lat_range"][0], dest_zone_info["lon_range"][1]),
            (dest_zone_info["lat_range"][1], dest_zone_info["lon_range"][0]),
            (dest_zone_info["lat_range"][1], dest_zone_info["lon_range"][1])
        ]
        
        # Find corner farthest from start
        max_dist = 0
        best_corner = None
        for corner in corners:
            dist = haversine_distance(start_lat, start_lon, corner[0], corner[1])
            if dist > max_dist:
                max_dist = dist
                best_corner = corner
        
        if best_corner:
            # Add small random variation
            lat = best_corner[0] + np.random.uniform(-0.001, 0.001)
            lon = best_corner[1] + np.random.uniform(-0.001, 0.001)
            return round(lat, 6), round(lon, 6)
    
    # For short trips or if all else fails, use random coordinates in zone
    lat = np.random.uniform(dest_zone_info["lat_range"][0], dest_zone_info["lat_range"][1])
    lon = np.random.uniform(dest_zone_info["lon_range"][0], dest_zone_info["lon_range"][1])
    return round(lat, 6), round(lon, 6)



# Optimized version for better performance
def add_coordinates_with_distance_matching_optimized(taxi_df):
    """
    Optimized version using vectorization where possible
    """
    print("Adding distance-aware coordinates to taxi data (optimized)...")
    
    # Initialize new columns
    taxi_df['PULatitude'] = np.nan
    taxi_df['PULongitude'] = np.nan
    taxi_df['DOLatitude'] = np.nan
    taxi_df['DOLongitude'] = np.nan
    
    # Filter valid trips
    valid_trips = taxi_df[
        (taxi_df['trip_distance'] > 0) & 
        (taxi_df['PULocationID'].isin(nyc_taxi_zone_coordinates.keys())) &
        (taxi_df['DOLocationID'].isin(nyc_taxi_zone_coordinates.keys()))
    ].copy()
    
    print(f"Processing {len(valid_trips):,} valid trips out of {len(taxi_df):,} total trips...")
    
    # Process in batches for memory efficiency
    batch_size = 1000
    for i in range(0, len(valid_trips), batch_size):
        batch = valid_trips.iloc[i:i+batch_size]
        
        for idx, row in batch.iterrows():
            # Get zone info
            pu_zone = nyc_taxi_zone_coordinates[row['PULocationID']]
            do_zone = nyc_taxi_zone_coordinates[row['DOLocationID']]
            
            if not (pu_zone["lat_range"] and pu_zone["lon_range"] and 
                   do_zone["lat_range"] and do_zone["lon_range"]):
                continue
            
            # Generate pickup coordinates
            pu_lat = np.random.uniform(pu_zone["lat_range"][0], pu_zone["lat_range"][1])
            pu_lon = np.random.uniform(pu_zone["lon_range"][0], pu_zone["lon_range"][1])
            
            # Generate distance-matched dropoff coordinates
            do_lat, do_lon = generate_destination_coordinates(
                pu_lat, pu_lon, row['trip_distance'], do_zone
            )
            
            if do_lat and do_lon:
                taxi_df.at[idx, 'PULatitude'] = pu_lat
                taxi_df.at[idx, 'PULongitude'] = pu_lon
                taxi_df.at[idx, 'DOLatitude'] = do_lat
                taxi_df.at[idx, 'DOLongitude'] = do_lon
        
        if i % 10000 == 0:
            print(f"Processed {i:,} / {len(valid_trips):,} valid trips...")
    
    print("Completed!")
    return taxi_df
