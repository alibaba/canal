package com.alibaba.otter.canal.parse.driver.mysql.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * mysql collation转换mapping关系表
 * 
 * @author agapple 2018年11月5日 下午1:01:15
 * @since 1.1.2
 */
public class CharsetUtil {

    private static final String[]             INDEX_TO_CHARSET = new String[2048];
    private static final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<String, Integer>();
    static {
        INDEX_TO_CHARSET[1] = "big5";
        INDEX_TO_CHARSET[84] = "big5";

        INDEX_TO_CHARSET[3] = "dec8";
        INDEX_TO_CHARSET[69] = "dec8";

        INDEX_TO_CHARSET[4] = "cp850";
        INDEX_TO_CHARSET[80] = "cp850";

        INDEX_TO_CHARSET[6] = "hp8";
        INDEX_TO_CHARSET[72] = "hp8";

        INDEX_TO_CHARSET[7] = "koi8r";
        INDEX_TO_CHARSET[74] = "koi8r";

        INDEX_TO_CHARSET[5] = "latin1";
        INDEX_TO_CHARSET[8] = "latin1";
        INDEX_TO_CHARSET[15] = "latin1";
        INDEX_TO_CHARSET[31] = "latin1";
        INDEX_TO_CHARSET[47] = "latin1";
        INDEX_TO_CHARSET[48] = "latin1";
        INDEX_TO_CHARSET[49] = "latin1";
        INDEX_TO_CHARSET[94] = "latin1";

        INDEX_TO_CHARSET[9] = "latin2";
        INDEX_TO_CHARSET[21] = "latin2";
        INDEX_TO_CHARSET[27] = "latin2";
        INDEX_TO_CHARSET[77] = "latin2";

        INDEX_TO_CHARSET[10] = "swe7";
        INDEX_TO_CHARSET[82] = "swe7";

        INDEX_TO_CHARSET[11] = "ascii";
        INDEX_TO_CHARSET[65] = "ascii";

        INDEX_TO_CHARSET[12] = "ujis";
        INDEX_TO_CHARSET[91] = "ujis";

        INDEX_TO_CHARSET[13] = "sjis";
        INDEX_TO_CHARSET[88] = "sjis";

        INDEX_TO_CHARSET[16] = "hebrew";
        INDEX_TO_CHARSET[71] = "hebrew";

        INDEX_TO_CHARSET[18] = "tis620";
        INDEX_TO_CHARSET[69] = "tis620";

        INDEX_TO_CHARSET[19] = "euckr";
        INDEX_TO_CHARSET[85] = "euckr";

        INDEX_TO_CHARSET[22] = "koi8u";
        INDEX_TO_CHARSET[75] = "koi8u";

        INDEX_TO_CHARSET[24] = "gb2312";
        INDEX_TO_CHARSET[86] = "gb2312";

        INDEX_TO_CHARSET[25] = "greek";
        INDEX_TO_CHARSET[70] = "greek";

        INDEX_TO_CHARSET[26] = "cp1250";
        INDEX_TO_CHARSET[34] = "cp1250";
        INDEX_TO_CHARSET[44] = "cp1250";
        INDEX_TO_CHARSET[66] = "cp1250";
        INDEX_TO_CHARSET[99] = "cp1250";

        INDEX_TO_CHARSET[28] = "gbk";
        INDEX_TO_CHARSET[87] = "gbk";

        INDEX_TO_CHARSET[30] = "latin5";
        INDEX_TO_CHARSET[78] = "latin5";

        INDEX_TO_CHARSET[32] = "armscii8";
        INDEX_TO_CHARSET[64] = "armscii8";

        INDEX_TO_CHARSET[33] = "utf8";
        INDEX_TO_CHARSET[83] = "utf8";
        for (int i = 192; i <= 223; i++) {
            INDEX_TO_CHARSET[i] = "utf8";
        }
        for (int i = 336; i <= 337; i++) {
            INDEX_TO_CHARSET[i] = "utf8";
        }
        for (int i = 352; i <= 357; i++) {
            INDEX_TO_CHARSET[i] = "utf8";
        }
        INDEX_TO_CHARSET[368] = "utf8";
        INDEX_TO_CHARSET[2047] = "utf8";

        INDEX_TO_CHARSET[35] = "ucs2";
        INDEX_TO_CHARSET[90] = "ucs2";
        for (int i = 128; i <= 151; i++) {
            INDEX_TO_CHARSET[i] = "ucs2";
        }
        INDEX_TO_CHARSET[159] = "ucs2";
        for (int i = 358; i <= 360; i++) {
            INDEX_TO_CHARSET[i] = "ucs2";
        }

        INDEX_TO_CHARSET[36] = "cp866";
        INDEX_TO_CHARSET[68] = "cp866";

        INDEX_TO_CHARSET[37] = "keybcs2";
        INDEX_TO_CHARSET[73] = "keybcs2";

        INDEX_TO_CHARSET[38] = "macce";
        INDEX_TO_CHARSET[43] = "macce";

        INDEX_TO_CHARSET[39] = "macroman";
        INDEX_TO_CHARSET[53] = "macroman";

        INDEX_TO_CHARSET[40] = "cp852";
        INDEX_TO_CHARSET[81] = "cp852";

        INDEX_TO_CHARSET[20] = "latin7";
        INDEX_TO_CHARSET[41] = "latin7";
        INDEX_TO_CHARSET[42] = "latin7";
        INDEX_TO_CHARSET[79] = "latin7";

        INDEX_TO_CHARSET[45] = "utf8mb4";
        INDEX_TO_CHARSET[46] = "utf8mb4";
        for (int i = 224; i <= 247; i++) {
            INDEX_TO_CHARSET[i] = "utf8mb4";
        }
        for (int i = 255; i <= 271; i++) {
            INDEX_TO_CHARSET[i] = "utf8mb4";
        }
        for (int i = 273; i <= 275; i++) {
            INDEX_TO_CHARSET[i] = "utf8mb4";
        }
        for (int i = 277; i <= 294; i++) {
            INDEX_TO_CHARSET[i] = "utf8mb4";
        }
        for (int i = 296; i <= 298; i++) {
            INDEX_TO_CHARSET[i] = "utf8mb4";
        }
        INDEX_TO_CHARSET[300] = "utf8mb4";
        for (int i = 303; i <= 307; i++) {
            INDEX_TO_CHARSET[i] = "utf8mb4";
        }
        INDEX_TO_CHARSET[326] = "utf8mb4";
        INDEX_TO_CHARSET[328] = "utf8mb4";

        INDEX_TO_CHARSET[14] = "cp1251";
        INDEX_TO_CHARSET[23] = "cp1251";
        INDEX_TO_CHARSET[50] = "cp1251";
        INDEX_TO_CHARSET[51] = "cp1251";
        INDEX_TO_CHARSET[52] = "cp1251";

        INDEX_TO_CHARSET[54] = "utf16";
        INDEX_TO_CHARSET[55] = "utf16";
        for (int i = 101; i <= 124; i++) {
            INDEX_TO_CHARSET[i] = "utf16";
        }
        INDEX_TO_CHARSET[327] = "utf16";

        INDEX_TO_CHARSET[56] = "utf16le";
        INDEX_TO_CHARSET[62] = "utf16le";

        INDEX_TO_CHARSET[57] = "cp1256";
        INDEX_TO_CHARSET[67] = "cp1256";

        INDEX_TO_CHARSET[29] = "cp1257";
        INDEX_TO_CHARSET[58] = "cp1257";
        INDEX_TO_CHARSET[59] = "cp1257";

        INDEX_TO_CHARSET[60] = "utf32";
        INDEX_TO_CHARSET[61] = "utf32";
        for (int i = 160; i <= 183; i++) {
            INDEX_TO_CHARSET[i] = "utf32";
        }
        INDEX_TO_CHARSET[391] = "utf32";

        INDEX_TO_CHARSET[63] = "binary";

        INDEX_TO_CHARSET[92] = "geostd8";
        INDEX_TO_CHARSET[93] = "geostd8";

        INDEX_TO_CHARSET[95] = "cp932";
        INDEX_TO_CHARSET[96] = "cp932";

        INDEX_TO_CHARSET[97] = "eucjpms";
        INDEX_TO_CHARSET[98] = "eucjpms";

        for (int i = 248; i <= 250; i++) {
            INDEX_TO_CHARSET[i] = "gb18030";
        }

        // charset --> index
        for (int i = 0; i < 2048; i++) {
            String charset = INDEX_TO_CHARSET[i];
            if (charset != null && CHARSET_TO_INDEX.get(charset) == null) {
                CHARSET_TO_INDEX.put(charset, i);
            }
        }
        CHARSET_TO_INDEX.put("iso-8859-1", 14);
        CHARSET_TO_INDEX.put("iso_8859_1", 14);
        CHARSET_TO_INDEX.put("utf-8", 33);
        CHARSET_TO_INDEX.put("utf8mb4", 45);
    }

    public static final String getCharset(int index) {
        return INDEX_TO_CHARSET[index];
    }

    public static final int getIndex(String charset) {
        if (charset == null || charset.length() == 0) {
            return 0;
        } else {
            Integer i = CHARSET_TO_INDEX.get(charset.toLowerCase());
            return (i == null) ? 0 : i.intValue();
        }
    }

    /**
     * 'utf8' COLLATE 'utf8_general_ci'
     * 
     * @param charset
     * @return
     */
    public static final String collateCharset(String charset) {
        String[] output = StringUtils.split(charset, "COLLATE");
        return output[0].replace('\'', ' ').trim();
    }

    public static String getJavaCharset(String charset) {
        if ("utf8".equals(charset)) {
            return charset;
        }

        if (StringUtils.endsWithIgnoreCase(charset, "utf8mb4")) {
            return "utf-8";
        }

        if (StringUtils.endsWithIgnoreCase(charset, "binary")) {
            return "iso_8859_1";
        }

        return charset;
    }
}
