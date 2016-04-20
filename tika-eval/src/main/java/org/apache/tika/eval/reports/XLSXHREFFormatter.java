package org.apache.tika.eval.reports;

import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * Created by TALLISON on 4/20/2016.
 */
public class XLSXHREFFormatter implements XSLXCellFormatter {
    //xlsx files can only have this many hyperlinks
    //if they have more Excel can't read the file
    private static final int MAX_HYPERLINKS = 65000;


    private final String urlBase;
    private final int linkType;
    private XSSFWorkbook workbook;
    private CellStyle style;
    private int links = 0;

    public XLSXHREFFormatter(String urlBase,
                             int linkType) {
        this.urlBase = urlBase;
        this.linkType = linkType;
    }

    @Override
    public void reset(XSSFWorkbook workbook) {
        this.workbook = workbook;
        style = workbook.createCellStyle();
        Font hlinkFont = workbook.createFont();
        hlinkFont.setUnderline(Font.U_SINGLE);
        hlinkFont.setColor(IndexedColors.BLUE.getIndex());
        style.setFont(hlinkFont);
        links = 0;

    }

    @Override
    public void applyStyleAndValue(int dbColNum, ResultSet resultSet, Cell cell) throws SQLException {
        if (links < MAX_HYPERLINKS) {
            Hyperlink hyperlink = workbook.getCreationHelper().createHyperlink(linkType);
            String path = resultSet.getString(dbColNum);
            String address = urlBase+path;
            hyperlink.setAddress(address);
            cell.setHyperlink(hyperlink);
            cell.setCellStyle(style);
            String fName = Paths.get(path).getFileName().toString();
            cell.setCellValue(fName);
            links++;
        } else {
            //silently stop adding hyperlinks
        }
    }
}
