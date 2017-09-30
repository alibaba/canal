package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

/**
 * Defines database change action types: INSERT, UPDATE, DELETE, OTHER.
 *
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public enum DBMSAction {

    INSERT('I'), UPDATE('U'), DELETE('D'), REPLACE('R'), OTHER('O'), CREATE('C'), ALTER('A'), ERASE('E'), QUERY('Q'),
    ROWQUERY('W'), TRUNCATE('T'), CINDEX('X'), DINDEX('Y'), RENAME('Z');

    protected final byte bValue;

    DBMSAction(char ch){
        this.bValue = (byte) ch;
    }

    /**
     * Return action type from byte value.
     */
    public static DBMSAction fromValue(int iValue) {
        switch ((char) iValue) {
            case 'I':
            case 'M': // MERGE (Oracle only)
                return INSERT;
            case 'U':
                return UPDATE;
            case 'D': // DELETE
                return DELETE;
            case 'R': // REPLACE
                return REPLACE;
            case 'C':
                return CREATE;
            case 'A':
                return ALTER;
            case 'E':
                return ERASE;
            case 'Q':
                return QUERY;
            case 'W':
                return ROWQUERY;
            case 'T':
                return TRUNCATE;
            case 'X':
                return CINDEX;
            case 'Y':
                return DINDEX;
            case 'Z':
                return RENAME;
        }
        return OTHER;
    }

    /**
     * Return action type from query.
     */
    public static DBMSAction fromQuery(String query) {
        int length = query.length();
        for (int index = 0; index < length; index++) {
            char ch = query.charAt(index);
            if (!Character.isWhitespace(ch)) {
                return DBMSAction.fromValue(ch);
            }
        }

        return OTHER;
    }

    /**
     * Return byte value of action type.
     */
    public byte value() {
        return bValue;
    }
}
