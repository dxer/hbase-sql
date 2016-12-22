package org.dxer.hbase.entity;

/**
 * Created by linghf on 2016/9/1.
 */

public class PageResult {
    private Integer currentPage;

    private Integer pageSize;

    private Integer totalCount;

    private Integer totalPage;

    private HResult[] results;

    public Integer getCurrentPage() {
        return currentPage;
    }

    public void setCurrentPage(Integer currentPage) {
        this.currentPage = currentPage;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }

    public Integer getTotalPage() {
        return totalPage;
    }

    public void setTotalPage(Integer totalPage) {
        this.totalPage = totalPage;
    }

    public HResult[] getResults() {
        return results;
    }

    public void setResults(HResult[] results) {
        this.results = results;
    }
}
