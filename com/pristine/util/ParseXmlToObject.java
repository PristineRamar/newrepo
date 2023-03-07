/*
 * Author : vaibhavkumar Start Date : Aug 14, 2009
 * 
 * Change Description Changed By Date
 * --------------------------------------------------------------
 */

package com.pristine.util;

import java.io.File;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import com.pristine.dto.VariationBaselineDTO;
import com.pristine.exception.GeneralException;

public class ParseXmlToObject
{
	static private Logger	logger	= Logger.getLogger("com.pristine.util.ParseXmlToObject");
	String	baseDir					= System.getenv("CATALINA_HOME")+File.separator +"webapps"+File.separator+"R-Project";
	String					outPut	= String.valueOf(PropertyManager.getProperty("R.OUTPUTDIR", "Output"));
	String					ROOT	= "VariationAnalysis";
	String					CHAIN	= "Chain";
	String					STORE	= "Store";
	String					AIV		= "AIV";
	String					SALE	= "Sale";
	String					REGNET	= "Reg-Net";
	String					DEPTH	= "Depth";
	String					PRODUCT	= "Product";
	String					ID		= "id";
	String					MEAN	= "Mean";
	String					MIN		= "Min";
	String					MAX		= "Max";
	String					ERROR	= "Error";

	public ArrayList<VariationBaselineDTO> getObjectFromXML() throws GeneralException
	{
		ArrayList<VariationBaselineDTO> variationBaseDtoList = null;

		try
		{
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			
			File file = new File(baseDir + File.separator + outPut + File.separator + "variation_baseline_output.XML");
			
			if (file.exists())
			{
				Document doc = (Document) docBuilder.parse(file);
				doc.normalize();

				// Getting the TagList(list of tags)having name
				// ROOT(VariationAnalysis)

				NodeList varAnalysis = doc.getElementsByTagName(ROOT);
				variationBaseDtoList = new ArrayList<VariationBaselineDTO>();

				Element root = (Element) varAnalysis.item(0);

				// Chain Level

				NodeList chains = root.getElementsByTagName(CHAIN);

				for (int c = 0; c < chains.getLength(); c++)
				{
					VariationBaselineDTO baselineDTO = new VariationBaselineDTO();

					Element chain = (Element) chains.item(c);
					baselineDTO.setChainID(new Integer(chain.getAttribute(ID)));
					logger.debug("CHAIN ID>>" + baselineDTO.getChainID());

					baselineDTO = getAvgMinMax(chain, AIV, baselineDTO);
					baselineDTO = getAvgMinMax(chain, SALE, baselineDTO);
					baselineDTO = getAvgMinMax(chain, REGNET, baselineDTO);
					baselineDTO = getAvgMinMax(chain, DEPTH, baselineDTO);

					// Store Level

					NodeList stores = chain.getElementsByTagName(STORE);

					logger.debug("No of Stores>>" + stores.getLength());

					NodeList products = chain.getElementsByTagName(PRODUCT);

					logger.debug("No of Products>>" + products.getLength());

					if (new Float(baselineDTO.getAiv()) != 0.0 || new Float(baselineDTO.getSale()) != 0.0 || new Float(baselineDTO.getDepth()) != 0.0 || new Float(baselineDTO.getNetReg()) != 0.0)
					{
						baselineDTO.setNumOfStores(stores.getLength());
						baselineDTO.setNumOfItems(products.getLength());
						variationBaseDtoList.add(baselineDTO);
					}

					for (int s = 0; s < stores.getLength(); s++)
					{
						Element store = (Element) stores.item(s);

						baselineDTO = new VariationBaselineDTO();
						baselineDTO.setChainID(new Integer(chain.getAttribute(ID)));
						baselineDTO.setStoreID(new Integer(store.getAttribute(ID)));
						baselineDTO = getAvgMinMax(store, AIV, baselineDTO);
						baselineDTO = getAvgMinMax(store, SALE, baselineDTO);
						baselineDTO = getAvgMinMax(store, REGNET, baselineDTO);
						baselineDTO = getAvgMinMax(store, DEPTH, baselineDTO);

						if (new Float(baselineDTO.getAiv()) != 0.0 || new Float(baselineDTO.getSale()) != 0.0 || new Float(baselineDTO.getDepth()) != 0.0 || new Float(baselineDTO.getNetReg()) != 0.0)
						{
							baselineDTO.setNumOfStores(stores.getLength());
							baselineDTO.setNumOfItems(products.getLength());
							variationBaseDtoList.add(baselineDTO);
						}
						// For Product Inside store

						NodeList storeProducts = store.getElementsByTagName(PRODUCT);

						if (storeProducts != null)
						{

							for (int sp = 0; sp < storeProducts.getLength(); sp++)
							{

								Element storePrd = (Element) storeProducts.item(sp);

								if (storePrd.getElementsByTagName(AIV) != null || storePrd.getElementsByTagName(REGNET) != null || storePrd.getElementsByTagName(SALE) != null || storePrd.getElementsByTagName(DEPTH) != null)
								{
									baselineDTO = new VariationBaselineDTO();

									baselineDTO.setChainID(new Integer(chain.getAttribute(ID)));
									baselineDTO.setStoreID(new Integer(store.getAttribute(ID)));
									baselineDTO.setDeptID(new Integer(storePrd.getAttribute(ID)));

									baselineDTO = getAvgMinMax(storePrd, AIV, baselineDTO);
									baselineDTO = getAvgMinMax(storePrd, SALE, baselineDTO);
									baselineDTO = getAvgMinMax(storePrd, REGNET, baselineDTO);
									baselineDTO = getAvgMinMax(storePrd, DEPTH, baselineDTO);

									if (new Float(baselineDTO.getAiv()) != 0.0 || new Float(baselineDTO.getSale()) != 0.0 || new Float(baselineDTO.getDepth()) != 0.0 || new Float(baselineDTO.getNetReg()) != 0.0)
									{
										baselineDTO.setNumOfStores(stores.getLength());
										baselineDTO.setNumOfItems(products.getLength());
										variationBaseDtoList.add(baselineDTO);
									}
								}
							}

						}

					}

					// Product Level

					for (int p = 0; p < products.getLength(); p++)
					{
						Element product = (Element) products.item(p);
						if (product.getElementsByTagName(AIV) != null || product.getElementsByTagName(REGNET) != null || product.getElementsByTagName(SALE) != null || product.getElementsByTagName(DEPTH) != null)
						{
							baselineDTO = new VariationBaselineDTO();
							baselineDTO.setChainID(new Integer(chain.getAttribute(ID)));
							baselineDTO.setDeptID(new Integer(product.getAttribute(ID)));
							baselineDTO = getAvgMinMax(product, AIV, baselineDTO);
							baselineDTO = getAvgMinMax(product, SALE, baselineDTO);
							baselineDTO = getAvgMinMax(product, REGNET, baselineDTO);
							baselineDTO = getAvgMinMax(product, DEPTH, baselineDTO);

							if (new Float(baselineDTO.getAiv()) != 0.0 || new Float(baselineDTO.getSale()) != 0.0 || new Float(baselineDTO.getDepth()) != 0.0 || new Float(baselineDTO.getNetReg()) != 0.0)
							{
								baselineDTO.setNumOfStores(stores.getLength());
								baselineDTO.setNumOfItems(products.getLength());
								variationBaseDtoList.add(baselineDTO);
							}
						}
					} // End of PRODUCT for loop
					logger.info("Size of BaseLines is >>" + variationBaseDtoList.size());
				}// End of CHAIN for loop
			} else
			{
				logger.info("getObjectFromXML-->File>>" + file.getName() + " doesnt exists");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return variationBaseDtoList;
	}

	/**
	 * This function parses the common part of AIV,Reg,Sale and Depth parameters
	 * 
	 * @param root
	 * @param type
	 * @param baselineDTO
	 * @return
	 */

	private VariationBaselineDTO getAvgMinMax(Element root, String type, VariationBaselineDTO baselineDTO)
	{

		try
		{

			NodeList nodeList = root.getElementsByTagName(type);

			Element element = (Element) nodeList.item(0);
			logger.debug("***Processing for Element >>" + root.getNodeName() + "-->ID>>" + root.getAttribute(ID) + "****");

			if (element != null)
			{
				NodeList mean = element.getElementsByTagName(MEAN);

				if (mean != null)
				{

					if (type.equals(AIV))
					{
						baselineDTO.setAiv(new Float(mean.item(0).getTextContent()));
						logger.debug(type + " VALUE>>" + baselineDTO.getAiv());
					} else if (type.equals(SALE))
					{
						baselineDTO.setSale(new Float(mean.item(0).getTextContent()));
						logger.debug(type + "  VALUE>>" + baselineDTO.getSale());
					} else if (type.equals(REGNET))
					{
						baselineDTO.setNetReg(new Float(mean.item(0).getTextContent()));
						logger.debug(type + " VALUE>>" + baselineDTO.getNetReg());
					} else if (type.equals(DEPTH))
					{
						baselineDTO.setDepth(new Float(mean.item(0).getTextContent()));
						logger.debug(type + "  VALUE>>" + baselineDTO.getDepth());
					}
				}

				NodeList min = element.getElementsByTagName(MIN);

				if (min != null)
				{

					if (type.equals(AIV))
					{
						baselineDTO.setAivLowerLimit(new Float(min.item(0).getTextContent()));
						logger.debug(type + " LOWER VALUE>>" + baselineDTO.getAivLowerLimit());
					} else if (type.equals(SALE))
					{
						baselineDTO.setSaleLowerLimit(new Float(min.item(0).getTextContent()));
						logger.debug(type + " LOWER VALUE>>" + baselineDTO.getSaleLowerLimit());
					} else if (type.equals(REGNET))
					{
						baselineDTO.setNetRegLowerLimit(new Float(min.item(0).getTextContent()));
						logger.debug(type + " LOWER VALUE>>" + baselineDTO.getNetRegLowerLimit());
					} else if (type.equals(DEPTH))
					{
						baselineDTO.setDepthLowerLimit(new Float(min.item(0).getTextContent()));
						logger.debug(type + " LOWER VALUE>>" + baselineDTO.getDepthLowerLimit());
					}
				}

				NodeList max = element.getElementsByTagName(MAX);

				if (max != null)
				{

					if (type.equals(AIV))
					{
						baselineDTO.setAivUpperLimit(new Float(max.item(0).getTextContent()));
						logger.debug(type + " UPPER VALUE>>" + baselineDTO.getAivUpperLimit());
					} else if (type.equals(SALE))
					{
						baselineDTO.setSaleUpperLimit(new Float(max.item(0).getTextContent()));
						logger.debug(type + " UPPER VALUE>>" + baselineDTO.getSaleUpperLimit());
					} else if (type.equals(REGNET))
					{
						baselineDTO.setNetRegUpperLimit(new Float(max.item(0).getTextContent()));
						logger.debug(type + " UPPER VALUE>>" + baselineDTO.getNetRegUpperLimit());
					} else if (type.equals(DEPTH))
					{
						baselineDTO.setDepthUpperLimit(new Float(max.item(0).getTextContent()));
						logger.debug(type + " UPPER VALUE>>" + baselineDTO.getDepthUpperLimit());
					}
				}
			} else
			{

				if (type.equals(AIV))
				{
					baselineDTO.setAiv(new Float(0.0));
					baselineDTO.setAivLowerLimit(new Float(0.0));
					baselineDTO.setAivUpperLimit(new Float(0.0));

				} else if (type.equals(REGNET))
				{
					baselineDTO.setNetReg(new Float(0.0));
					baselineDTO.setNetRegLowerLimit(new Float(0.0));
					baselineDTO.setNetRegUpperLimit(new Float(0.0));

				} else if (type.equals(SALE))
				{
					baselineDTO.setSale(new Float(0.0));
					baselineDTO.setSaleLowerLimit(new Float(0.0));
					baselineDTO.setSaleUpperLimit(new Float(0.0));

				} else if (type.equals(DEPTH))
				{
					baselineDTO.setDepth(new Float(0.0));
					baselineDTO.setDepthLowerLimit(new Float(0.0));
					baselineDTO.setDepthUpperLimit(new Float(0.0));
				}

			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return baselineDTO;

	}

}
