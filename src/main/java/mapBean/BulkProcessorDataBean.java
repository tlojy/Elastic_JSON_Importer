package mapBean;

import java.util.ArrayList;
import java.util.List;

public class BulkProcessorDataBean {
	
	private List<IndexPropBean> indexProps;
	private List<String> dataSourses;
	
	public BulkProcessorDataBean() {
		this.indexProps = new ArrayList<IndexPropBean>();
		this.dataSourses = new ArrayList<String>();
	}
	
	public List<IndexPropBean> getIndexProps() {
		return indexProps;
	}
	public void setIndexProps(List<IndexPropBean> indexProps) {
		this.indexProps = indexProps;
	}
	public List<String> getDataSourses() {
		return dataSourses;
	}
	public void setDataSourses(List<String> dataSourses) {
		this.dataSourses = dataSourses;
	}
	

}
