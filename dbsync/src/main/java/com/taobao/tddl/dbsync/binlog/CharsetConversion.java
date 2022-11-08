package com.taobao.tddl.dbsync.binlog;

import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An utility class implements MySQL/Java charsets conversion. you can see
 * com.mysql.jdbc.CharsetMapping.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 */
public final class CharsetConversion {

    static final Log logger = LogFactory.getLog(CharsetConversion.class);

    static final class Entry {

        protected final int    charsetId;
        protected final String mysqlCharset;
        protected final String mysqlCollation;
        protected final String javaCharset;

        Entry(final int id, String mysqlCharset, // NL
              String mysqlCollation, String javaCharset){
            this.charsetId = id;
            this.mysqlCharset = mysqlCharset;
            this.mysqlCollation = mysqlCollation;
            this.javaCharset = javaCharset;
        }
    }

    // Character set data used in lookups. The array will be sparse.
    static final Entry[] entries = new Entry[2048];

    static Entry getEntry(final int id) {
        if (id >= 0 && id < entries.length) {
            return entries[id];
        } else {
            throw new IllegalArgumentException("Invalid charset id: " + id);
        }
    }

    // Loads character set information.
    static void putEntry(final int charsetId, String mysqlCharset, String mysqlCollation, String javaCharset) {
        entries[charsetId] = new Entry(charsetId, mysqlCharset, // NL
            mysqlCollation,
            javaCharset);
    }

    // Loads character set information.
    @Deprecated
    static void putEntry(final int charsetId, String mysqlCharset, String mysqlCollation) {
        entries[charsetId] = new Entry(charsetId, mysqlCharset, // NL
            mysqlCollation, /* Unknown java charset */
            null);
    }

    // Load character set data statically.
    static {
        putEntry(1, "big5", "big5_chinese_ci", "Big5");
        putEntry(2, "latin2", "latin2_czech_cs", "ISO8859_2");
        putEntry(3, "dec8", "dec8_swedish_ci", "ISO8859_1");
        putEntry(4, "cp850", "cp850_general_ci", "Cp850");
        putEntry(5, "latin1", "latin1_german1_ci", "ISO8859_1");
        putEntry(6, "hp8", "hp8_english_ci", "ISO8859_1");
        putEntry(7, "koi8r", "koi8r_general_ci", "KOI8_R");
        putEntry(8, "latin1", "latin1_swedish_ci", "ISO8859_1");
        putEntry(9, "latin2", "latin2_general_ci", "ISO8859_2");
        putEntry(10, "swe7", "swe7_swedish_ci", "ISO8859_1");
        putEntry(11, "ascii", "ascii_general_ci", "US-ASCII");
        putEntry(12, "ujis", "ujis_japanese_ci", "EUC_JP");
        putEntry(13, "sjis", "sjis_japanese_ci", "SJIS");
        putEntry(14, "cp1251", "cp1251_bulgarian_ci", "Cp1251");
        putEntry(15, "latin1", "latin1_danish_ci", "ISO8859_1");
        putEntry(16, "hebrew", "hebrew_general_ci", "ISO8859_8");
        putEntry(17, "filename", "filename", "ISO8859_1");
        putEntry(18, "tis620", "tis620_thai_ci", "TIS620");
        putEntry(19, "euckr", "euckr_korean_ci", "EUC_KR");
        putEntry(20, "latin7", "latin7_estonian_cs", "ISO8859_7");
        putEntry(21, "latin2", "latin2_hungarian_ci", "ISO8859_2");
        putEntry(22, "koi8u", "koi8u_general_ci", "KOI8_U");
        putEntry(23, "cp1251", "cp1251_ukrainian_ci", "Cp1251");
        putEntry(24, "gb2312", "gb2312_chinese_ci", "EUC_CN");
        putEntry(25, "greek", "greek_general_ci", "ISO8859_7");
        putEntry(26, "cp1250", "cp1250_general_ci", "Cp1250");
        putEntry(27, "latin2", "latin2_croatian_ci", "ISO8859_2");
        putEntry(28, "gbk", "gbk_chinese_ci", "GBK");
        putEntry(29, "cp1257", "cp1257_lithuanian_ci", "Cp1257");
        putEntry(30, "latin5", "latin5_turkish_ci", "ISO8859_5");
        putEntry(31, "latin1", "latin1_german2_ci", "ISO8859_1");
        putEntry(32, "armscii8", "armscii8_general_ci", "ISO8859_1");
        putEntry(33, "utf8", "utf8_general_ci", "UTF-8");
        putEntry(34, "cp1250", "cp1250_czech_cs", "Cp1250");
        putEntry(35, "ucs2", "ucs2_general_ci", "UnicodeBig");
        putEntry(36, "cp866", "cp866_general_ci", "Cp866");
        putEntry(37, "keybcs2", "keybcs2_general_ci", "Cp852");
        putEntry(38, "macce", "macce_general_ci", "MacCentralEurope");
        putEntry(39, "macroman", "macroman_general_ci", "MacRoman");
        putEntry(40, "cp852", "cp852_general_ci", "Cp852");
        putEntry(41, "latin7", "latin7_general_ci", "ISO8859_7");
        putEntry(42, "latin7", "latin7_general_cs", "ISO8859_7");
        putEntry(43, "macce", "macce_bin", "MacCentralEurope");
        putEntry(44, "cp1250", "cp1250_croatian_ci", "Cp1250");
        putEntry(45, "utf8mb4", "utf8mb4_general_ci", "UTF-8");
        putEntry(46, "utf8mb4", "utf8mb4_bin", "UTF-8");
        putEntry(47, "latin1", "latin1_bin", "ISO8859_1");
        putEntry(48, "latin1", "latin1_general_ci", "ISO8859_1");
        putEntry(49, "latin1", "latin1_general_cs", "ISO8859_1");
        putEntry(50, "cp1251", "cp1251_bin", "Cp1251");
        putEntry(51, "cp1251", "cp1251_general_ci", "Cp1251");
        putEntry(52, "cp1251", "cp1251_general_cs", "Cp1251");
        putEntry(53, "macroman", "macroman_bin", "MacRoman");
        putEntry(54, "utf16", "utf16_general_ci", "UTF-16");
        putEntry(55, "utf16", "utf16_bin", "UTF-16");
        putEntry(57, "cp1256", "cp1256_general_ci", "Cp1256");
        putEntry(58, "cp1257", "cp1257_bin", "Cp1257");
        putEntry(59, "cp1257", "cp1257_general_ci", "Cp1257");
        putEntry(60, "utf32", "utf32_general_ci", "UTF-32");
        putEntry(61, "utf32", "utf32_bin", "UTF-32");
        putEntry(63, "binary", "binary", "US-ASCII");
        putEntry(64, "armscii8", "armscii8_bin", "ISO8859_2");
        putEntry(65, "ascii", "ascii_bin", "US-ASCII");
        putEntry(66, "cp1250", "cp1250_bin", "Cp1250");
        putEntry(67, "cp1256", "cp1256_bin", "Cp1256");
        putEntry(68, "cp866", "cp866_bin", "Cp866");
        putEntry(69, "dec8", "dec8_bin", "US-ASCII");
        putEntry(70, "greek", "greek_bin", "ISO8859_7");
        putEntry(71, "hebrew", "hebrew_bin", "ISO8859_8");
        putEntry(72, "hp8", "hp8_bin", "US-ASCII");
        putEntry(73, "keybcs2", "keybcs2_bin", "Cp852");
        putEntry(74, "koi8r", "koi8r_bin", "KOI8_R");
        putEntry(75, "koi8u", "koi8u_bin", "KOI8_U");
        putEntry(77, "latin2", "latin2_bin", "ISO8859_2");
        putEntry(78, "latin5", "latin5_bin", "ISO8859_5");
        putEntry(79, "latin7", "latin7_bin", "ISO8859_7");
        putEntry(80, "cp850", "cp850_bin", "Cp850");
        putEntry(81, "cp852", "cp852_bin", "Cp852");
        putEntry(82, "swe7", "swe7_bin", "ISO8859_1");
        putEntry(83, "utf8", "utf8_bin", "UTF-8");
        putEntry(84, "big5", "big5_bin", "Big5");
        putEntry(85, "euckr", "euckr_bin", "EUC_KR");
        putEntry(86, "gb2312", "gb2312_bin", "EUC_CN");
        putEntry(87, "gbk", "gbk_bin", "GBK");
        putEntry(88, "sjis", "sjis_bin", "SJIS");
        putEntry(89, "tis620", "tis620_bin", "TIS620");
        putEntry(90, "ucs2", "ucs2_bin", "UnicodeBig");
        putEntry(91, "ujis", "ujis_bin", "EUC_JP");
        putEntry(92, "geostd8", "geostd8_general_ci", "US-ASCII");
        putEntry(93, "geostd8", "geostd8_bin", "US-ASCII");
        putEntry(94, "latin1", "latin1_spanish_ci", "ISO8859_1");
        putEntry(95, "cp932", "cp932_japanese_ci", "Shift_JIS");
        putEntry(96, "cp932", "cp932_bin", "Shift_JIS");
        putEntry(97, "eucjpms", "eucjpms_japanese_ci", "EUC_JP");
        putEntry(98, "eucjpms", "eucjpms_bin", "EUC_JP");
        putEntry(99, "cp1250", "cp1250_polish_ci", "Cp1250");

        putEntry(101, "utf16", "utf16_unicode_ci", "UTF-16");
        putEntry(102, "utf16", "utf16_icelandic_ci", "UTF-16");
        putEntry(103, "utf16", "utf16_latvian_ci", "UTF-16");
        putEntry(104, "utf16", "utf16_romanian_ci", "UTF-16");
        putEntry(105, "utf16", "utf16_slovenian_ci", "UTF-16");
        putEntry(106, "utf16", "utf16_polish_ci", "UTF-16");
        putEntry(107, "utf16", "utf16_estonian_ci", "UTF-16");
        putEntry(108, "utf16", "utf16_spanish_ci", "UTF-16");
        putEntry(109, "utf16", "utf16_swedish_ci", "UTF-16");
        putEntry(110, "utf16", "utf16_turkish_ci", "UTF-16");
        putEntry(111, "utf16", "utf16_czech_ci", "UTF-16");
        putEntry(112, "utf16", "utf16_danish_ci", "UTF-16");
        putEntry(113, "utf16", "utf16_lithuanian_ci", "UTF-16");
        putEntry(114, "utf16", "utf16_slovak_ci", "UTF-16");
        putEntry(115, "utf16", "utf16_spanish2_ci", "UTF-16");
        putEntry(116, "utf16", "utf16_roman_ci", "UTF-16");
        putEntry(117, "utf16", "utf16_persian_ci", "UTF-16");
        putEntry(118, "utf16", "utf16_esperanto_ci", "UTF-16");
        putEntry(119, "utf16", "utf16_hungarian_ci", "UTF-16");
        putEntry(120, "utf16", "utf16_sinhala_ci", "UTF-16");

        putEntry(128, "ucs2", "ucs2_unicode_ci", "UnicodeBig");
        putEntry(129, "ucs2", "ucs2_icelandic_ci", "UnicodeBig");
        putEntry(130, "ucs2", "ucs2_latvian_ci", "UnicodeBig");
        putEntry(131, "ucs2", "ucs2_romanian_ci", "UnicodeBig");
        putEntry(132, "ucs2", "ucs2_slovenian_ci", "UnicodeBig");
        putEntry(133, "ucs2", "ucs2_polish_ci", "UnicodeBig");
        putEntry(134, "ucs2", "ucs2_estonian_ci", "UnicodeBig");
        putEntry(135, "ucs2", "ucs2_spanish_ci", "UnicodeBig");
        putEntry(136, "ucs2", "ucs2_swedish_ci", "UnicodeBig");
        putEntry(137, "ucs2", "ucs2_turkish_ci", "UnicodeBig");
        putEntry(138, "ucs2", "ucs2_czech_ci", "UnicodeBig");
        putEntry(139, "ucs2", "ucs2_danish_ci", "UnicodeBig");
        putEntry(140, "ucs2", "ucs2_lithuanian_ci", "UnicodeBig");
        putEntry(141, "ucs2", "ucs2_slovak_ci", "UnicodeBig");
        putEntry(142, "ucs2", "ucs2_spanish2_ci", "UnicodeBig");
        putEntry(143, "ucs2", "ucs2_roman_ci", "UnicodeBig");
        putEntry(144, "ucs2", "ucs2_persian_ci", "UnicodeBig");
        putEntry(145, "ucs2", "ucs2_esperanto_ci", "UnicodeBig");
        putEntry(146, "ucs2", "ucs2_hungarian_ci", "UnicodeBig");
        putEntry(147, "ucs2", "ucs2_sinhala_ci", "UnicodeBig");

        putEntry(160, "utf32", "utf32_unicode_ci", "UTF-32");
        putEntry(161, "utf32", "utf32_icelandic_ci", "UTF-32");
        putEntry(162, "utf32", "utf32_latvian_ci", "UTF-32");
        putEntry(163, "utf32", "utf32_romanian_ci", "UTF-32");
        putEntry(164, "utf32", "utf32_slovenian_ci", "UTF-32");
        putEntry(165, "utf32", "utf32_polish_ci", "UTF-32");
        putEntry(166, "utf32", "utf32_estonian_ci", "UTF-32");
        putEntry(167, "utf32", "utf32_spanish_ci", "UTF-32");
        putEntry(168, "utf32", "utf32_swedish_ci", "UTF-32");
        putEntry(169, "utf32", "utf32_turkish_ci", "UTF-32");
        putEntry(170, "utf32", "utf32_czech_ci", "UTF-32");
        putEntry(171, "utf32", "utf32_danish_ci", "UTF-32");
        putEntry(172, "utf32", "utf32_lithuanian_ci", "UTF-32");
        putEntry(173, "utf32", "utf32_slovak_ci", "UTF-32");
        putEntry(174, "utf32", "utf32_spanish2_ci", "UTF-32");
        putEntry(175, "utf32", "utf32_roman_ci", "UTF-32");
        putEntry(176, "utf32", "utf32_persian_ci", "UTF-32");
        putEntry(177, "utf32", "utf32_esperanto_ci", "UTF-32");
        putEntry(178, "utf32", "utf32_hungarian_ci", "UTF-32");
        putEntry(179, "utf32", "utf32_sinhala_ci", "UTF-32");

        putEntry(192, "utf8", "utf8_unicode_ci", "UTF-8");
        putEntry(193, "utf8", "utf8_icelandic_ci", "UTF-8");
        putEntry(194, "utf8", "utf8_latvian_ci", "UTF-8");
        putEntry(195, "utf8", "utf8_romanian_ci", "UTF-8");
        putEntry(196, "utf8", "utf8_slovenian_ci", "UTF-8");
        putEntry(197, "utf8", "utf8_polish_ci", "UTF-8");
        putEntry(198, "utf8", "utf8_estonian_ci", "UTF-8");
        putEntry(199, "utf8", "utf8_spanish_ci", "UTF-8");
        putEntry(200, "utf8", "utf8_swedish_ci", "UTF-8");
        putEntry(201, "utf8", "utf8_turkish_ci", "UTF-8");
        putEntry(202, "utf8", "utf8_czech_ci", "UTF-8");
        putEntry(203, "utf8", "utf8_danish_ci", "UTF-8");
        putEntry(204, "utf8", "utf8_lithuanian_ci", "UTF-8");
        putEntry(205, "utf8", "utf8_slovak_ci", "UTF-8");
        putEntry(206, "utf8", "utf8_spanish2_ci", "UTF-8");
        putEntry(207, "utf8", "utf8_roman_ci", "UTF-8");
        putEntry(208, "utf8", "utf8_persian_ci", "UTF-8");
        putEntry(209, "utf8", "utf8_esperanto_ci", "UTF-8");
        putEntry(210, "utf8", "utf8_hungarian_ci", "UTF-8");
        putEntry(211, "utf8", "utf8_sinhala_ci", "UTF-8");

        putEntry(224, "utf8mb4", "utf8mb4_unicode_ci", "UTF-8");
        putEntry(225, "utf8mb4", "utf8mb4_icelandic_ci", "UTF-8");
        putEntry(226, "utf8mb4", "utf8mb4_latvian_ci", "UTF-8");
        putEntry(227, "utf8mb4", "utf8mb4_romanian_ci", "UTF-8");
        putEntry(228, "utf8mb4", "utf8mb4_slovenian_ci", "UTF-8");
        putEntry(229, "utf8mb4", "utf8mb4_polish_ci", "UTF-8");
        putEntry(230, "utf8mb4", "utf8mb4_estonian_ci", "UTF-8");
        putEntry(231, "utf8mb4", "utf8mb4_spanish_ci", "UTF-8");
        putEntry(232, "utf8mb4", "utf8mb4_swedish_ci", "UTF-8");
        putEntry(233, "utf8mb4", "utf8mb4_turkish_ci", "UTF-8");
        putEntry(234, "utf8mb4", "utf8mb4_czech_ci", "UTF-8");
        putEntry(235, "utf8mb4", "utf8mb4_danish_ci", "UTF-8");
        putEntry(236, "utf8mb4", "utf8mb4_lithuanian_ci", "UTF-8");
        putEntry(237, "utf8mb4", "utf8mb4_slovak_ci", "UTF-8");
        putEntry(238, "utf8mb4", "utf8mb4_spanish2_ci", "UTF-8");
        putEntry(239, "utf8mb4", "utf8mb4_roman_ci", "UTF-8");
        putEntry(240, "utf8mb4", "utf8mb4_persian_ci", "UTF-8");
        putEntry(241, "utf8mb4", "utf8mb4_esperanto_ci", "UTF-8");
        putEntry(242, "utf8mb4", "utf8mb4_hungarian_ci", "UTF-8");
        putEntry(243, "utf8mb4", "utf8mb4_sinhala_ci", "UTF-8");
        putEntry(244, "utf8mb4", "utf8mb4_german2_ci", "UTF-8");
        putEntry(245, "utf8mb4", "utf8mb4_croatian_ci", "UTF-8");
        putEntry(246, "utf8mb4", "utf8mb4_unicode_520_ci", "UTF-8");
        putEntry(247, "utf8mb4", "utf8mb4_vietnamese_ci", "UTF-8");
        putEntry(248, "gb18030", "gb18030_chinese_ci", "GB18030");
        putEntry(249, "gb18030", "gb18030_bin", "GB18030");
        putEntry(250, "gb18030", "gb18030_unicode_520_ci", "GB18030");

        putEntry(254, "utf8", "utf8_general_cs", "UTF-8");
        putEntry(255, "utf8mb4", "utf8mb4_0900_ai_ci", "UTF-8");
        putEntry(256, "utf8mb4", "utf8mb4_de_pb_0900_ai_ci", "UTF-8");
        putEntry(257, "utf8mb4", "utf8mb4_is_0900_ai_ci", "UTF-8");
        putEntry(258, "utf8mb4", "utf8mb4_lv_0900_ai_ci", "UTF-8");
        putEntry(259, "utf8mb4", "utf8mb4_ro_0900_ai_ci", "UTF-8");
        putEntry(260, "utf8mb4", "utf8mb4_sl_0900_ai_ci", "UTF-8");
        putEntry(261, "utf8mb4", "utf8mb4_pl_0900_ai_ci", "UTF-8");
        putEntry(262, "utf8mb4", "utf8mb4_et_0900_ai_ci", "UTF-8");
        putEntry(263, "utf8mb4", "utf8mb4_es_0900_ai_ci", "UTF-8");
        putEntry(264, "utf8mb4", "utf8mb4_sv_0900_ai_ci", "UTF-8");
        putEntry(265, "utf8mb4", "utf8mb4_tr_0900_ai_ci", "UTF-8");
        putEntry(266, "utf8mb4", "utf8mb4_cs_0900_ai_ci", "UTF-8");
        putEntry(267, "utf8mb4", "utf8mb4_da_0900_ai_ci", "UTF-8");
        putEntry(268, "utf8mb4", "utf8mb4_lt_0900_ai_ci", "UTF-8");
        putEntry(269, "utf8mb4", "utf8mb4_sk_0900_ai_ci", "UTF-8");
        putEntry(270, "utf8mb4", "utf8mb4_es_trad_0900_ai_ci", "UTF-8");
        putEntry(271, "utf8mb4", "utf8mb4_la_0900_ai_ci", "UTF-8");

        putEntry(273, "utf8mb4", "utf8mb4_eo_0900_ai_ci", "UTF-8");
        putEntry(274, "utf8mb4", "utf8mb4_hu_0900_ai_ci", "UTF-8");
        putEntry(275, "utf8mb4", "utf8mb4_hr_0900_ai_ci", "UTF-8");

        putEntry(277, "utf8mb4", "utf8mb4_vi_0900_ai_ci", "UTF-8");
        putEntry(278, "utf8mb4", "utf8mb4_0900_as_cs", "UTF-8");
        putEntry(279, "utf8mb4", "utf8mb4_de_pb_0900_as_cs", "UTF-8");
        putEntry(280, "utf8mb4", "utf8mb4_is_0900_as_cs", "UTF-8");
        putEntry(281, "utf8mb4", "utf8mb4_lv_0900_as_cs", "UTF-8");
        putEntry(282, "utf8mb4", "utf8mb4_ro_0900_as_cs", "UTF-8");
        putEntry(283, "utf8mb4", "utf8mb4_sl_0900_as_cs", "UTF-8");
        putEntry(284, "utf8mb4", "utf8mb4_pl_0900_as_cs", "UTF-8");
        putEntry(285, "utf8mb4", "utf8mb4_et_0900_as_cs", "UTF-8");
        putEntry(286, "utf8mb4", "utf8mb4_es_0900_as_cs", "UTF-8");
        putEntry(287, "utf8mb4", "utf8mb4_sv_0900_as_cs", "UTF-8");
        putEntry(288, "utf8mb4", "utf8mb4_tr_0900_as_cs", "UTF-8");
        putEntry(289, "utf8mb4", "utf8mb4_cs_0900_as_cs", "UTF-8");
        putEntry(290, "utf8mb4", "utf8mb4_da_0900_as_cs", "UTF-8");
        putEntry(291, "utf8mb4", "utf8mb4_lt_0900_as_cs", "UTF-8");
        putEntry(292, "utf8mb4", "utf8mb4_sk_0900_as_cs", "UTF-8");
        putEntry(293, "utf8mb4", "utf8mb4_es_trad_0900_as_cs", "UTF-8");
        putEntry(294, "utf8mb4", "utf8mb4_la_0900_as_cs", "UTF-8");

        putEntry(296, "utf8mb4", "utf8mb4_eo_0900_as_cs", "UTF-8");
        putEntry(297, "utf8mb4", "utf8mb4_hu_0900_as_cs", "UTF-8");
        putEntry(298, "utf8mb4", "utf8mb4_hr_0900_as_cs", "UTF-8");

        putEntry(300, "utf8mb4", "utf8mb4_vi_0900_as_cs", "UTF-8");
        putEntry(303, "utf8mb4", "utf8mb4_ja_0900_as_cs", "UTF-8");
        putEntry(304, "utf8mb4", "utf8mb4_ja_0900_as_cs_ks", "UTF-8");
        putEntry(305, "utf8mb4", "utf8mb4_0900_as_ci", "UTF-8");
        putEntry(306, "utf8mb4", "utf8mb4_ru_0900_ai_ci", "UTF-8");
        putEntry(307, "utf8mb4", "utf8mb4_ru_0900_as_cs", "UTF-8");

        putEntry(326, "utf8mb4", "utf8mb4_test_ci", "UTF-8");
        putEntry(327, "utf16", "utf16_test_ci", "UTF-16");
        putEntry(328, "utf8mb4", "utf8mb4_test_400_ci", "UTF-8");

        putEntry(336, "utf8", "utf8_bengali_standard_ci", "UTF-8");
        putEntry(337, "utf8", "utf8_bengali_standard_ci", "UTF-8");
        putEntry(352, "utf8", "utf8_phone_ci", "UTF-8");
        putEntry(353, "utf8", "utf8_test_ci", "UTF-8");
        putEntry(354, "utf8", "utf8_5624_1", "UTF-8");
        putEntry(355, "utf8", "utf8_5624_2", "UTF-8");
        putEntry(356, "utf8", "utf8_5624_3", "UTF-8");
        putEntry(357, "utf8", "utf8_5624_4", "UTF-8");
        putEntry(358, "ucs2", "ucs2_test_ci", "UnicodeBig");
        putEntry(359, "ucs2", "ucs2_vn_ci", "UnicodeBig");
        putEntry(360, "ucs2", "ucs2_5624_1", "UnicodeBig");

        putEntry(368, "utf8", "utf8_5624_5", "UTF-8");
        putEntry(391, "utf32", "utf32_test_ci", "UTF-32");
        putEntry(2047, "utf8", "utf8_maxuserid_ci", "UTF-8");
    }

    /**
     * Return defined charset name for mysql.
     */
    public static String getCharset(final int id) {
        Entry entry = getEntry(id);

        if (entry != null) {
            return entry.mysqlCharset;
        } else {
            logger.warn("Unexpect mysql charset: " + id);
            return null;
        }
    }

    /**
     * Return defined collaction name for mysql.
     */
    public static String getCollation(final int id) {
        Entry entry = getEntry(id);

        if (entry != null) {
            return entry.mysqlCollation;
        } else {
            logger.warn("Unexpect mysql charset: " + id);
            return null;
        }
    }

    /**
     * Return converted charset name for java.
     */
    public static String getJavaCharset(final int id) {
        Entry entry = getEntry(id);

        if (entry != null) {
            if (entry.javaCharset != null) {
                return entry.javaCharset;
            } else {
                logger.warn("Unknown java charset for: id = " + id + ", name = " + entry.mysqlCharset + ", coll = "
                            + entry.mysqlCollation);
                return null;
            }
        } else {
            logger.warn("Unexpect mysql charset: " + id);
            return null;
        }
    }

    public static void main(String[] args) {
        for (int i = 0; i < entries.length; i++) {
            Entry entry = entries[i];

            System.out.print(i);
            System.out.print(',');
            System.out.print(' ');
            if (entry != null) {
                System.out.print(entry.mysqlCharset);
                System.out.print(',');
                System.out.print(' ');
                System.out.print(entry.javaCharset);
                if (entry.javaCharset != null) {
                    System.out.print(',');
                    System.out.print(' ');
                    System.out.print(Charset.forName(entry.javaCharset).name());
                }
            } else {
                System.out.print("null");
            }
            System.out.println();
        }
    }
}
