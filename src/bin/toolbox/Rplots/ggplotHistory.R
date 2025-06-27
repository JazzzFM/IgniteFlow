library(tidyverse)
library(lubridate)
library(officer)
library(glue)
library(xtable)

############### [ to_vectorname function ] ###########################

# This function takes a dataframe and a string that is a name of a column
# Returns a vector (a list for pythoners) with the distinct elements in that column
to_vectorname <- function(df, name){
    v <- df %>%
        select(name) %>%
        distinct() %>%
        pull()

    return(v)
}

############### [ plot_counts function ] ###########################

# This function takes a dataframe and a string that is a name of a column
# Returns a ggplot with the counts of that table per month and a arima - model of that 
plot_counts <- function(df){
    # We get the name of the table
    table_name <- df %>% select(table_name) %>% distinct() %>% pull()
    # With this we get the upper limit of the plot in y-axis and we add a little bit more
    max_count <- df %>% select(count) %>% pull() %>% max()
    max_count <- max_count + max_count/10
    # With this we get the lower limit of the plot in y-axis and we add a little bit more
    min_count <- df %>% select(count) %>% pull() %>% min()
    min_count <- min_count - min_count/10
    # With this we get the upper limit of the plot in x-axis and we add 3 months more to rigth
    max_date <- df %>% select(partition) %>% pull() %>% ymd()%>% max() + months(3)
    # With this we get the lower limit of the plot in x-axis and we add 3 months more to left
    min_date <- df %>% select(partition) %>% pull() %>% ymd() %>% min() - months(3)
    # This vector returns a list of dates from the minimal to the maximal
    vect_breaks <- seq(min_count, max_count, by = (max_count-min_count)/30)
    # This function returns the date interval as real numbers
    inter <- interval(min_date, max_date)
    # This is the mid date of all tables
    mid_date <- int_start(inter) + (int_end(inter) - int_start(inter))/2
    
    # Get the labels
    ejec <- df %>% to_vectorname("execution_date")
    pivot <- df %>% to_vectorname("cohort_date")
    win_min <- df %>% to_vectorname("window_start")
    win_max <- df %>% to_vectorname("window_end")
    list_df <- df %>% split(.$alias)

    # Let's create the plot 
    Graph <- df %>%
            ggplot(aes(x = partition, y = count, label = NumRegistros )) +
            geom_smooth(method = "glm", color = "#D7E5F0", size = 1.5) +
            geom_line(color = "#104B84", size = 1.5) +
            geom_point(size = 2.5, color = "red") +
            theme_minimal() +
            ylim(min_count, max_count) +
            xlim(min_date, max_date) +
            scale_x_date(date_breaks = "1 month", date_labels = "%Y-%m") +
            scale_y_continuous(label = scales::comma, breaks = vect_breaks) +
            ggrepel::geom_label_repel(
                fill = "white",
                fontface = "bold",
                box.padding = 0.3,
                point.padding = 0.5,
                min.segment.length = 0.5,
                segment.color = "grey50",
                nudge_x = 0.08) +
            theme(axis.text.x = element_text(angle = 60, hjust=1))+
            labs(title = glue("Tabla: {table_name}"),
                subtitle = glue("Fecha Pivote: {pivot}, Fecha Ejecución: {ejec}"),
                caption = glue("Ventana de Tiempo: {win_min} - {win_max}")) +
            xlab("Fecha Partición") +
            ylab("Num. Registros")

    return(Graph)
}


############### [ main function ] ###########################

main <- function(path_data, path_plots, first_view){
    # Print the arguments
    print(glue("path_data: {path_data}"))
    print(glue("path_plots: {path_plots}"))
    print(glue("first_view: {first_view}"))

    # Import and Transform Data
    df_query <- readr::read_csv(path_data) %>%
                arrange(partition) %>%
                mutate(
                    partition = lubridate::ymd(partition),
                    NumRegistros = scales::number(count, big.mark = ","),
                    fecha_pivote = as.character(partition))

    # Split a Dataframes in a list of dataframes
    list_df <- df_query %>%
                split(.$alias)

    # Generate pdf plots    
    for(x in 1:length(list_df)){
        plot <- list_df[[x]] %>% 
            plot_counts()

        glue::glue("{path_plots}/plot_{x}.pdf") %>%
        ggsave()
    }

    
    html_summary <- list_df %>%
        map(~ .x %>%
            arrange(partition) %>%
            mutate(
                pct_diff = abs((count)/lag(count)),
                color = case_when(
                    abs(pct_diff) > 0.75 & abs(pct_diff) < 1.25 ~ "#639754", 
                    abs(pct_diff) >= 0.50 &  abs(pct_diff) <= 0.75 ~ "#FFD301",
                    abs(pct_diff) >= 1.25 &  abs(pct_diff) >= 1.75 ~ "#FFD301",
                    abs(pct_diff) < 0.50 ~ "#D611F1F",
                    abs(pct_diff) > 1.75 ~ "#D61F1F", 
                    is.na(pct_diff) ~ "#808080"), 
                grow_rate = glue("<font color='{color}'> <strong> {pct_diff} </strong> </font>")) %>%
        filter(partition == max(partition)) %>%
        select(table_name, fecha_pivote, NumRegistros, grow_rate)) %>% 
        bind_rows() 

    html_summary %>% 
        xtable() %>%
        print(type = "html", file = glue::glue("{first_view}"))

    print(html_summary)
    print(glue("first_view: {first_view}"))

}


############### [ Entry Point ] ###########################

# Asing the parameters
args = commandArgs(trailingOnly = TRUE)
path_data <- args[1]
path_plots <- args[2]
first_view <- args[3]

# Excecute the main function
main(path_data, path_plots, first_view)
