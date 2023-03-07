package com.pristine.service.offermgmt.substitute;

import java.util.List;

import com.pristine.dto.offermgmt.substitute.SubstituteAdjRInputDTO;
import com.pristine.dto.offermgmt.substitute.SubstituteAdjROutputDTO;
import com.pristine.exception.GeneralException;

public interface SubstituteAdjustmentRService {

	public List<SubstituteAdjROutputDTO> getSubsAdjustedMov(SubstituteAdjRInputDTO substituteInput) throws GeneralException;
}
