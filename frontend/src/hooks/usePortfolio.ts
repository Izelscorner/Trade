/** Portfolio hook — DB-backed via react-query */

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  fetchPortfolio,
  addToPortfolio,
  removeFromPortfolio,
} from "../api/client";

export function usePortfolio() {
  const queryClient = useQueryClient();

  const { data: portfolioIds = [] } = useQuery({
    queryKey: ["portfolio"],
    queryFn: fetchPortfolio,
    staleTime: 60_000,
  });

  const addMutation = useMutation({
    mutationFn: addToPortfolio,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["portfolio"] }),
  });

  const removeMutation = useMutation({
    mutationFn: removeFromPortfolio,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["portfolio"] }),
  });

  const isInPortfolio = (id: string) => portfolioIds.includes(id);

  const togglePortfolio = (id: string) => {
    if (isInPortfolio(id)) {
      removeMutation.mutate(id);
    } else {
      addMutation.mutate(id);
    }
  };

  return { portfolioIds, isInPortfolio, togglePortfolio };
}
