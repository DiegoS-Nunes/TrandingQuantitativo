from fastapi import HTTPException
from ..services.dfp_service import DfpService
from ..utils.update_response import UpdateResponse

class DfpController:
    def get_dfp(self) -> UpdateResponse:
        try:
            result = dfp_service().update_dfp_data()
            if not result:
                return UpdateResponse(
                    success=False,
                    message="DFP data not updated",
                    details=result
                )
            return UpdateResponse(
                success=True,
                message="DFP data updated!",
                details=result
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))