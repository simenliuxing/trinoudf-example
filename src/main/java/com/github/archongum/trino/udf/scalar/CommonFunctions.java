package com.github.archongum.trino.udf.scalar;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.airlift.slice.Slices.utf8Slice;


/**
 * Random with string type seed
 *
 * @author Archon  2018/9/20
 * @since
 */
public class CommonFunctions {

    @Description("get_full_acct_no(String value1)")
    @ScalarFunction("get_full_acct_no")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice evaluate(@SqlType(StandardTypes.VARCHAR) Slice value1) {
        StringBuilder fullAcctNo = new StringBuilder();
        String checktable = "1009080706050403020510040903080207010805021007040109060408010509020610030204060810010305070102030405060708090601070208030904100306090104071002050703100602090501080907050301100806041009080780605040302051004090308020701080502100704010906040801050902061003020406081001030507010203040506070809";
        String value = value1.toStringUtf8();

        try {
            String tempAcct = value + "0";
            int rawlen = tempAcct.length();
//            System.out.println(rawlen);
            if (rawlen == 17) {
                int sub1 = rawlen - 1;
                int sub2;
                Integer workValue = 0;
                while (sub1 >= 0) {
                    if (!tempAcct.substring(sub1, sub1 + 1).equals("0")) {
                        sub2 = Integer.parseInt(tempAcct.substring(sub1, sub1 + 1));
                        workValue = workValue + Integer.parseInt(checktable.substring(sub1 * 18 + sub2 * 2 - 2, sub1 * 18 + sub2 * 2));
                    }
                    sub1 = sub1 - 1;
                }
                if (workValue != 0) {
                    workValue = workValue % 10;
                    fullAcctNo.append(value.substring(0, 16)).append(workValue.toString().trim());
                }
            } else {
                System.out.println("account no length error");
            }
        } catch (Exception e) {
            System.out.println("get full account no error");
        }
        return utf8Slice(fullAcctNo.toString());
    }

    @Description("getStrX(String value)")
    @ScalarFunction("getStrX")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getStrX(@SqlType(StandardTypes.VARCHAR) Slice value) {
        return utf8Slice(value.toStringUtf8() + "--XXX");
    }

    public static void main(String[] args) {
        String value = "1075000000160109";
        System.out.println(value.length());
        System.out.println("913301055630355491".substring(8, 17));
        System.out.println("full account no:" + evaluate(value));
    }

    public static String evaluate(String value) {
        StringBuilder fullAcctNo = new StringBuilder();
        String checktable = "1009080706050403020510040903080207010805021007040109060408010509020610030204060810010305070102030405060708090601070208030904100306090104071002050703100602090501080907050301100806041009080780605040302051004090308020701080502100704010906040801050902061003020406081001030507010203040506070809";
        try {
            String tempAcct = value + "0";
            int rawlen = tempAcct.length();
//            System.out.println(rawlen);
            if (rawlen == 17) {
                int sub1 = rawlen - 1;
                int sub2;
                Integer workValue = 0;
                while (sub1 >= 0) {
                    if (!tempAcct.substring(sub1, sub1 + 1).equals("0")) {
                        sub2 = Integer.parseInt(tempAcct.substring(sub1, sub1 + 1));
                        workValue = workValue + Integer.parseInt(checktable.substring(sub1 * 18 + sub2 * 2 - 2, sub1 * 18 + sub2 * 2));
                    }
                    sub1 = sub1 - 1;
                }
                if (workValue != 0) {
                    workValue = workValue % 10;
                    fullAcctNo.append(value.substring(0, 16)).append(workValue.toString().trim());
                }
            } else {
                System.out.println("account no length error");
            }
        } catch (Exception e) {
            System.out.println("get full account no error");
        }
        return (fullAcctNo.toString());
    }
}
